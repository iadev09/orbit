set shell := ["bash", "--noprofile", "--norc", "-euo", "pipefail", "-c"]

default:
    @just --list

# Native Linux smoke suite used by xd01 and Gitea Actions.
smoke:
    test "$(uname -s)" = "Linux"
    rustc --version
    cargo --version
    cargo test --locked -p orbit-rs --all-targets
