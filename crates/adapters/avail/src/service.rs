use std::time::Duration;

use anyhow::anyhow;
use avail_rust_client::avail::data_availability::tx::SubmitData;
use avail_rust_client::avail_rust_core::AppId;
use avail_rust_client::avail_rust_core::rpc::kate::{DataProof, TxDataRoots};
use avail_rust_client::avail_rust_core::rpc::system::fetch_extrinsics_v1_types::{
    EncodeSelector, SignatureFilter,
};
use avail_rust_client::subxt_core::tx;
use avail_rust_client::{
    AccountId, AccountIdExt, Client, H256, HashNumber, Keypair, KeypairExt, Options,
    TransactionDecodable,
};
use backon::ExponentialBuilder;
use reqwest::Client as rClient;
use sov_rollup_interface::common::HexHash;
use sov_rollup_interface::da::{DaProof, DaSpec, RelevantBlobs, RelevantProofs, Time};
use sov_rollup_interface::node::da::{
    DaService, MaybeRetryable, SubmitBlobReceipt, run_maybe_retryable_async_fn_with_retries,
};
use sp_core::blake2_256;
use tokio::sync::oneshot;
use tracing::{debug, error, info, instrument, trace, warn};

use crate::types::address::AvailAddress;
use crate::types::blob::AvailDABlob;
use crate::types::block::AvailBlock;
use crate::types::config::AvailDAConfig;
use crate::types::data::AvailData;
use crate::types::header::CustomAvailHeader;
use crate::types::utils::CustomTransaction;
use crate::verifier::{AvailDASpec, AvailDAVerifier};

#[derive(Clone)]
pub struct AvailDAService {
    pub client: Client,
    pub proof_app_id: AppId,
    pub batch_app_id: AppId,
    pub signer: Keypair,

    pub turbo_da_enabled: bool,
    pub turbo_da_url: String,
    pub turbo_da_api_key: String,

    backoff_policy: ExponentialBuilder,
}

#[async_trait::async_trait]
impl DaService for AvailDAService {
    type Spec = AvailDASpec;
    type Config = AvailDAConfig;
    type Verifier = AvailDAVerifier;
    type FilteredBlock = AvailBlock;
    type Error = anyhow::Error;

    #[instrument(skip(self))]
    async fn get_block_at(&self, height: u64) -> Result<Self::FilteredBlock, Self::Error> {
        info!("Attempting to get block at height: {}", height);
        run_maybe_retryable_async_fn_with_retries(
            &self.backoff_policy,
            || async {
                let height_u32 = u32::try_from(height)
                    .map_err(|_| anyhow!("Block height {} does not fit in u32", height))?;
                self.get_block_at_height(height_u32)
                    .await
                    .map_err(MaybeRetryable::Transient)
            },
            "get_block_at",
        )
        .await
    }

    #[instrument(skip(self))]
    async fn get_last_finalized_block_header(
        &self,
    ) -> Result<<Self::Spec as DaSpec>::BlockHeader, Self::Error> {
        info!("Attempting to get last finalized block header");
        run_maybe_retryable_async_fn_with_retries(
            &self.backoff_policy,
            || async {
                self.get_finalized_block_header()
                    .await
                    .map_err(MaybeRetryable::Transient)
            },
            "get_last_finalised_block_header",
        )
        .await
    }

    #[instrument(skip(self))]
    async fn get_head_block_header(
        &self,
    ) -> Result<<Self::Spec as DaSpec>::BlockHeader, Self::Error> {
        info!("Attempting to get head block header");
        run_maybe_retryable_async_fn_with_retries(
            &self.backoff_policy,
            || async {
                self.get_best_block_header()
                    .await
                    .map_err(MaybeRetryable::Transient)
            },
            "get_head_block_header",
        )
        .await
    }

    #[instrument(skip(self, block))]
    fn extract_relevant_blobs(
        &self,
        block: &Self::FilteredBlock,
    ) -> RelevantBlobs<<Self::Spec as DaSpec>::BlobTransaction> {
        info!(
            "Extracting relevant blobs from block number: {}",
            block.header.header.number
        );

        let batch_blobs =
            self.convert_blobs(&block.batch_blobs, "batch", block.header.header.number);
        let proof_blobs =
            self.convert_blobs(&block.proof_blobs, "proof", block.header.header.number);

        info!(
            "Extracted {} batch blobs and {} proof blobs from block number: {}",
            batch_blobs.len(),
            proof_blobs.len(),
            block.header.header.number
        );
        RelevantBlobs {
            batch_blobs,
            proof_blobs,
        }
    }

    #[instrument(skip(self, block, _blobs))]
    async fn get_extraction_proof(
        &self,
        block: &Self::FilteredBlock,
        _blobs: &RelevantBlobs<<Self::Spec as DaSpec>::BlobTransaction>,
    ) -> RelevantProofs<
        <Self::Spec as DaSpec>::InclusionMultiProof,
        <Self::Spec as DaSpec>::CompletenessProof,
    > {
        error!("get_extraction_proof method is not implemented for AvailDA yet");
        info!(
            "Getting extraction proof for block number: {}",
            block.header.header.number
        );
        let dummy_proof = DataProof {
            roots: TxDataRoots {
                data_root: H256::zero(),
                blob_root: H256::zero(),
                bridge_root: H256::zero(),
            },
            proof: vec![],
            number_of_leaves: 1,
            leaf_index: 0,
            leaf: H256::zero(),
        };
        trace!("Created dummy proof: {:?}", dummy_proof);

        info!(
            "Returning dummy extraction proof for block number: {}",
            block.header.header.number
        );

        RelevantProofs {
            batch: DaProof {
                inclusion_proof: dummy_proof.clone(),
                completeness_proof: dummy_proof.clone(),
            },
            proof: DaProof {
                inclusion_proof: dummy_proof.clone(),
                completeness_proof: dummy_proof.clone(),
            },
        }
    }

    #[instrument(skip(self, blob))]
    async fn send_transaction(
        &self,
        blob: &[u8],
    ) -> oneshot::Receiver<
        Result<SubmitBlobReceipt<<Self::Spec as DaSpec>::TransactionId>, Self::Error>,
    > {
        info!(
            "Sending batch blob on AvailDA with blob of size: {} bytes",
            blob.len()
        );
        let (tx, rx) = oneshot::channel();
        let result = run_maybe_retryable_async_fn_with_retries(
            &self.backoff_policy,
            || async {
                self.submit_data(self.batch_app_id, blob)
                    .await
                    .map(|tx_hash: H256| SubmitBlobReceipt {
                        blob_hash: HexHash::from(blake2_256(blob)),
                        da_transaction_id: tx_hash,
                    })
                    .map_err(MaybeRetryable::Transient)
            },
            "send_transaction",
        )
        .await;
        info!("Batch blob submission result: {:?}", result.is_ok());
        let _ = tx.send(result);
        rx
    }

    #[instrument(skip(self, aggregated_proof_data))]
    async fn send_proof(
        &self,
        aggregated_proof_data: &[u8],
    ) -> oneshot::Receiver<
        Result<SubmitBlobReceipt<<Self::Spec as DaSpec>::TransactionId>, Self::Error>,
    > {
        info!(
            "Sending proof blob on AvailDA with blob of size: {} bytes",
            aggregated_proof_data.len()
        );
        let (tx, rx) = oneshot::channel();
        let result = run_maybe_retryable_async_fn_with_retries(
            &self.backoff_policy,
            || async {
                self.submit_data(self.proof_app_id, aggregated_proof_data)
                    .await
                    .map(|tx_hash: H256| SubmitBlobReceipt {
                        blob_hash: HexHash::from(blake2_256(aggregated_proof_data)),
                        da_transaction_id: tx_hash,
                    })
                    .map_err(MaybeRetryable::Transient)
            },
            "send_proof",
        )
        .await;
        info!("Proof blob submission succeeded: {:?}", result.is_ok());
        let _ = tx.send(result);
        rx
    }

    #[instrument(skip(self))]
    async fn get_proofs_at(&self, height: u64) -> Result<Vec<Vec<u8>>, Self::Error> {
        error!(
            "get_proofs_at method is not implemented for AvailDA yet for height: {}",
            height
        );
        Err(anyhow::anyhow!(
            "get_proofs_at method is not implemented yet for AvailDA".to_string(),
        ))
    }

    #[instrument(skip(self))]
    async fn get_signer(&self) -> <Self::Spec as DaSpec>::Address {
        AvailAddress(self.signer.public_key().to_account_id())
    }
}

impl AvailDAService {
    #[instrument(skip(config))]
    pub async fn new_from_config(config: AvailDAConfig) -> Result<Self, anyhow::Error> {
        info!("Creating new AvailDAService from config");
        let client = Client::new(&config.http_api_url).await?;
        debug!("Avail client created for URL: {}", config.http_api_url);

        let account = Keypair::from_str(&config.signer_key).map_err(|err| {
            error!("Failed to create keypair from signer key: {}", err);
            anyhow::anyhow!(err)
        })?;
        debug!("Keypair created successfully");

        if config.turbo_da_enabled {
            info!("Turbo DA is enabled");
            debug!("Turbo DA url: {}", config.turbo_da_url);
            debug!("Turbo DA api key: {}", config.turbo_da_api_key);
        }

        // NOTE: Current exponential backoff policy defaults:
        // jitter: false, factor: 2, min_delay: 1s, max_delay: 60s, max_times: 3,
        let backoff_policy = ExponentialBuilder::default()
            .with_min_delay(Duration::from_secs(
                config.backoff_min_delay_secs.unwrap_or(1),
            ))
            .with_max_delay(Duration::from_secs(
                config.backoff_max_delay_secs.unwrap_or(60),
            ))
            .with_max_times(config.backoff_max_times.unwrap_or(3))
            .with_factor(config.backoff_factor.unwrap_or(2.0));
        debug!("Backoff policy initialized");

        info!("AvailDAService created successfully");
        Ok(AvailDAService {
            client,
            proof_app_id: AppId(config.proof_app_id),
            batch_app_id: AppId(config.batch_app_id),
            signer: account,
            turbo_da_enabled: config.turbo_da_enabled,
            turbo_da_url: config.turbo_da_url,
            turbo_da_api_key: config.turbo_da_api_key,
            backoff_policy,
        })
    }

    #[instrument(skip(self))]
    async fn get_finalized_block_header(&self) -> Result<CustomAvailHeader, anyhow::Error> {
        debug!("Fetching finalized block header");
        let header = self.client.finalized_block_header().await?;
        debug!("Finalized block header retrieved: {:?}", header.number);
        Ok(CustomAvailHeader { header })
    }

    #[instrument(skip(self))]
    async fn get_best_block_header(&self) -> Result<CustomAvailHeader, anyhow::Error> {
        debug!("Fetching best block header");
        let header = self.client.best_block_header().await?;
        debug!("Best block header retrieved: {:?}", header.number);
        Ok(CustomAvailHeader { header })
    }

    #[instrument(skip(self))]
    async fn get_block_at_height(&self, block_number: u32) -> Result<AvailBlock, anyhow::Error> {
        debug!("Fetching block at height: {}", block_number);

        debug!("Fetching block header for block number {}", block_number);
        let block_hash = self.client.block_hash(block_number).await?.ok_or_else(|| {
            anyhow::anyhow!("Block hash not found for block number {}", block_number)
        })?;
        trace!("Retrieved block hash: {:?}", block_hash);

        let block_client = self.client.block_client();

        let block_header = self.client.block_header(block_hash).await?.ok_or_else(|| {
            anyhow::anyhow!("Block header not found for block hash {:?}", block_hash)
        })?;
        debug!("Retrieved block header: {:?}", block_header);

        debug!("Fetching batch and proof blobs for block {}", block_number);
        let batch_blobs = self
            .fetch_and_process_blobs(block_number, self.batch_app_id)
            .await?;
        let proof_blobs = self
            .fetch_and_process_blobs(block_number, self.proof_app_id)
            .await?;
        debug!(
            "Retrieved {} batch blobs and {} proof blobs for block {}",
            batch_blobs.len(),
            proof_blobs.len(),
            block_number
        );

        // Fetching the first transaction of the block to get the timestamp
        debug!("Fetching timestamp transaction for block {}", block_number);
        let tx = block_client
            .block_transaction(
                block_hash.into(),
                0.into(),
                None,
                Some(EncodeSelector::Call),
            )
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("Transaction not found for block hash {:?}", block_hash)
            })?;
        trace!("Retrieved timestamp transaction: {:?}", tx);

        let encoded_call_str = tx
            .encoded
            .ok_or_else(|| anyhow::anyhow!("Encoded call not found in transaction"))?
            .to_string();
        trace!("Decoded transaction call string: {}", encoded_call_str);

        let custom_tx = CustomTransaction::decode_hex_call(&encoded_call_str)
            .ok_or_else(|| anyhow::anyhow!("Failed to decode custom transaction"))?;
        trace!("Custom transaction: {:?}", custom_tx);
        debug!(
            "Retrieved timestamp {} from transaction for block {} ",
            custom_tx.set, block_number
        );

        debug!(
            "Successfully processed Avail block of height {}",
            block_number
        );
        Ok(AvailBlock::new(
            CustomAvailHeader {
                header: block_header,
            },
            block_hash,
            Time::from_secs(custom_tx.set as i64),
            batch_blobs,
            proof_blobs,
        ))
    }

    #[instrument(skip(self, app_id, data))]
    async fn submit_data(&self, app_id: AppId, data: &[u8]) -> Result<H256, anyhow::Error> {
        debug!(
            "Submitting data of size {} bytes for app_id {}",
            data.len(),
            app_id.0
        );

        let res;
        if self.turbo_da_enabled {
            debug!("Submitting data via Turbo DA");

            let http_client = reqwest::Client::new();
            let result = self
                .submit_data_to_turbo_da(
                    &http_client,
                    data,
                    10, // max_attempts
                )
                .await?;

            debug!(
                "Turbo DA finalized at block {} with hash {:?}",
                result.block_number, result.block_hash
            );

            Ok(result.block_hash)
        } else {
            let submittable_tx = self
                .client
                .tx()
                .data_availability()
                .submit_data(data.to_vec());
            trace!("Submittable transaction created");
            let submitted_tx = submittable_tx
                .sign_and_submit(&self.signer, Options::new(Some(app_id.0)))
                .await?;

            // Fetching Transaction Receipt
            let receipt = submitted_tx.receipt(false).await?;
            let Some(receipt) = receipt else {
                return Err(anyhow!("Transaction got dropped."));
            };

            // Fetching Block State
            let block_state = receipt.block_state().await?;
            match block_state {
                avail_rust_client::BlockState::Included => {
                    debug!("Block is included but not yet finalized")
                }
                avail_rust_client::BlockState::Finalized => debug!("Block is finalized"),
                avail_rust_client::BlockState::Discarded => debug!("Block is discarded"),
                avail_rust_client::BlockState::DoesNotExist => debug!("Block does not exist"),
            }
            res = submitted_tx.tx_hash;
        }

        debug!(
            "Data submitted successfully, transaction hash: {:?} and blob hash: {:?}",
            res,
            HexHash::from(blake2_256(data))
        );
        Ok(res)
    }
}

// --- Response Structs ---
#[derive(Debug, Deserialize)]
struct TurboDAResponse {
    submission_id: String,
}

#[derive(Debug, Deserialize)]
struct TurboDAStatusResponse {
    error: String,
    id: String,
    state: String,
    data: Option<TurboDAStatusData>,
}

#[derive(Debug, Deserialize)]
struct TurboDAStatusData {
    block_number: u64,
    block_hash: String,
    tx_index: u64,
    tx_hash: String,
}

impl AvailDAService {
    fn convert_blobs(
        &self,
        blobs: &Vec<AvailData>,
        label: &str,
        block_number: u32,
    ) -> Vec<AvailDABlob> {
        let converted: Vec<AvailDABlob> = blobs.iter().map(AvailDABlob::from).collect();
        debug!(
            "Extracted {} {} blobs from block {}",
            converted.len(),
            label,
            block_number
        );
        converted
    }

    // Submits data to Turbo DA and polls until finalized
    async fn submit_data_to_turbo_da(
        &self,
        client: &rClient,
        data: &[u8],
        max_attempts: usize,
    ) -> Result<H256, anyhow::Error> {
        // Step 1: POST to /v1/submit_raw_data
        let post_url = format!("{}/v1/submit_raw_data", self.turbo_da_url);
        debug!("Submitting data to Turbo DA at: {}", post_url);

        let post_resp = client
            .post(&post_url)
            .header("Content-Type", "application/octet-stream")
            .header("x-api-key", self.turbo_da_api_key.clone())
            .body(data.to_vec())
            .send()
            .await
            .map_err(|e| anyhow!("failed to send Turbo DA POST: {}", e))?;

        if !post_resp.status().is_success() {
            let status = post_resp.status();
            let body = post_resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Turbo DA POST failed: status {} body: {}",
                status,
                body
            ));
        }

        let post_json: TurboDAResponse = post_resp.json().await?;
        let submission_id = post_json.submission_id;
        debug!("Turbo DA submission ID: {}", submission_id);

        // Step 2: Poll /v1/get_submission_info
        let get_url = format!(
            "{}/v1/get_submission_info?submission_id={}",
            self.turbo_da_url, submission_id
        );

        for attempt in 1..=max_attempts {
            let resp = client
                .get(&get_url)
                .header("x-api-key", self.turbo_da_api_key.clone())
                .send()
                .await;

            match resp {
                Ok(resp) => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();

                    if !status.is_success() {
                        warn!("Turbo DA status check failed: {}", status);
                    } else {
                        if let Ok(status_json) =
                            serde_json::from_str::<TurboDAStatusResponse>(&body)
                        {
                            debug!(
                                "Attempt {}/{} | State: {}",
                                attempt, max_attempts, status_json.state
                            );
                            if status_json.state == "Finalized" {
                                if let Some(data) = status_json.data {
                                    let tx_hash = H256::from_slice(&hex::decode(
                                        data.tx_hash.trim_start_matches("0x"),
                                    )?);
                                    debug!("Turbo DA finalized: transaction hash {:?}", tx_hash);
                                    return Ok(tx_hash);
                                } else {
                                    return Err(anyhow!(
                                        "Finalized but no data in Turbo DA response"
                                    ));
                                }
                            }
                        } else {
                            warn!("Invalid JSON from Turbo DA: {}", body);
                        }
                    }
                }
                Err(err) => {
                    warn!(
                        "Turbo DA request failed (attempt {}/{}): {:?}",
                        attempt, max_attempts, err
                    );
                }
            }

            // exponential backoff with cap
            let sleep_secs = std::cmp::min(30, 2 << attempt);
            debug!("Sleeping {}s before next poll...", sleep_secs);
            sleep(Duration::from_secs(sleep_secs as u64)).await;
        }

        Err(anyhow!(
            "Turbo DA submission {} did not finalize within {} attempts",
            submission_id,
            max_attempts
        ))
    }

    async fn fetch_and_process_blobs(
        &self,
        block_number: u32,
        app_id: AppId,
    ) -> Result<Vec<AvailData>, anyhow::Error> {
        trace!(
            "Fetching transactions for block number {} and app_id {}",
            block_number, app_id.0
        );
        let block_client = self.client.block_client();
        let extrinsics = block_client
            .block_transactions(
                HashNumber::Number(block_number),
                None,
                Some(SignatureFilter::new(None, Some(app_id.0), None)),
                Some(EncodeSelector::Call),
            )
            .await?;

        trace!("Found {} blobs for app_id {}", extrinsics.len(), app_id.0);

        let mut blobs = Vec::new();

        for item in extrinsics.iter() {
            // Extract encoded string
            let encoded_str = match &item.encoded {
                Some(s) => s.as_str(),
                None => {
                    warn!(
                        "Skipping blob: missing encoded data (block {}, app_id {})",
                        block_number, app_id.0
                    );
                    continue;
                }
            };

            // Try decoding
            let submit_data = match SubmitData::decode_hex_call(encoded_str) {
                Some(submit) => submit,
                None => {
                    warn!(
                        "Skipping blob: failed to decode hex call (block {}, app_id {}, data={})",
                        block_number, app_id.0, encoded_str
                    );
                    continue;
                }
            };

            // Extract signer
            let signer = match &item.signature {
                Some(sig) if sig.ss58_address.is_some() => {
                    match AccountId::from_str(sig.ss58_address.as_ref().unwrap()) {
                        Ok(account) => account,
                        Err(e) => {
                            warn!(
                                "Skipping blob: failed to parse AccountId (block {}, app_id {}, error={})",
                                block_number, app_id.0, e
                            );
                            continue;
                        }
                    }
                }
                _ => {
                    warn!(
                        "Skipping blob: missing signature (block {}, app_id {})",
                        block_number, app_id.0
                    );
                    continue;
                }
            };
            // If all went well, add it
            blobs.push(AvailData {
                data: submit_data.data,
                signer,
            });
        }

        trace!(
            "Successfully extracted {} blobs for app_id {} (out of {} transactions)",
            blobs.len(),
            app_id.0,
            extrinsics.len()
        );

        Ok(blobs)
    }
}
