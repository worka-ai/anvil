use std::process::{Command, Output};
use std::time::Duration;

pub(super) fn configured_docker_image() -> String {
    std::env::var("ANVIL_IMAGE").unwrap_or_else(|_| "anvil:test".to_string())
}

pub(super) fn require_docker_image() -> String {
    let image = configured_docker_image();
    let mut last_output = None;
    for attempt in 0..5 {
        let output = inspect_image(&image);
        if let Some(image_id) = inspected_image_id(&output) {
            return image_id;
        }
        last_output = Some(output);

        // Docker Desktop can transiently retain a valid repository/tag entry
        // while its tag resolver reports NoSuchImage. The immutable ID remains
        // usable and is the safer Compose input for the complete test run.
        if let Some(image_id) = image_id_from_listing(&image) {
            let by_id = inspect_image(&image_id);
            if inspected_image_id(&by_id).is_some() {
                return image_id;
            }
        }

        if attempt < 4 {
            std::thread::sleep(Duration::from_millis(250 * (attempt + 1) as u64));
        }
    }

    let output = last_output.expect("Docker image inspection attempted at least once");
    panic!(
        "Docker-backed Anvil tests require prebuilt Docker image `{image}`. Build it with \
         `./scripts/build-image.sh` or set ANVIL_IMAGE to an existing local tag or image ID.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn inspect_image(image: &str) -> Output {
    Command::new("docker")
        .args(["image", "inspect", "--format", "{{.Id}}", image])
        .output()
        .unwrap_or_else(|error| {
            panic!(
                "Docker-backed Anvil tests require Docker and prebuilt image `{image}`; \
                 failed to run `docker image inspect`: {error}"
            )
        })
}

fn inspected_image_id(output: &Output) -> Option<String> {
    if !output.status.success() {
        return None;
    }
    let image_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
    image_id.starts_with("sha256:").then_some(image_id)
}

fn image_id_from_listing(reference: &str) -> Option<String> {
    let output = Command::new("docker")
        .args([
            "image",
            "ls",
            "--no-trunc",
            "--format",
            "{{.Repository}}:{{.Tag}}\t{{.ID}}",
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_once('\t'))
        .find_map(|(listed_reference, image_id)| {
            (listed_reference == reference && image_id.starts_with("sha256:"))
                .then(|| image_id.to_string())
        })
}
