use sp1_build::{build_program_with_args, BuildArgs};

fn should_skip_guest_build() -> bool {
    match std::env::var("SKIP_GUEST_BUILD")
        .as_ref()
        .map(|arg0: &String| String::as_str(arg0))
    {
        Ok("1") | Ok("true") | Ok("sp1") => true,
        Ok("0") | Ok("false") | Ok(_) | Err(_) => false,
    }
}

fn main() {
    println!("cargo::rerun-if-env-changed=SKIP_GUEST_BUILD");
    println!("cargo::rerun-if-env-changed=OUT_DIR");

    if should_skip_guest_build() {
        println!("cargo:warning=Skipping sp1 guest build");
        return;
    }

    let mut features = vec![];

    if cfg!(feature = "bench") {
        features.push("bench".to_string());
    }

    build_program_with_args(
        "./guest-mock",
        BuildArgs {
            features: features.clone(),
            ..Default::default()
        },
    );
    build_program_with_args(
        "./guest-avail",
        BuildArgs {
            features: features.clone(),
            ..Default::default()
        },
    );
    build_program_with_args(
        "./guest-celestia",
        BuildArgs {
            features,
            ..Default::default()
        },
    );
}
