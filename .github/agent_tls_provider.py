from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}: {old[:160]!r}")
    p.write_text(text.replace(old, new, 1))


replace_once(
    "Cargo.toml",
    'axum-server = { version = "0.8", features = ["tls-rustls"] }\n',
    'axum-server = { version = "0.8", features = ["tls-rustls"] }\nrustls = { version = "0.23", default-features = false, features = ["aws_lc_rs"] }\n',
)

for manifest in ["apps/scirust-hubd/Cargo.toml", "apps/scirust-hub-worker/Cargo.toml"]:
    replace_once(
        manifest,
        'axum-server = { workspace = true }\n',
        'axum-server = { workspace = true }\nrustls = { workspace = true }\n',
    )

replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''fn run(args: Args) -> Result<(), DaemonError> {
    init_tracing();
''',
    '''fn install_rustls_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

fn run(args: Args) -> Result<(), DaemonError> {
    // Cargo feature unification can make both built-in Rustls providers
    // available through the server and HTTP-client dependency graph. Select
    // one process-wide provider before either server or remote-client TLS can
    // construct a Rustls config.
    install_rustls_crypto_provider();
    init_tracing();
''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''    #[test]
    fn tls_configuration_requires_cert_and_key_together() {
''',
    '''    #[test]
    fn rustls_crypto_provider_is_installed_explicitly() {
        install_rustls_crypto_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }

    #[test]
    fn tls_configuration_requires_cert_and_key_together() {
''',
)

replace_once(
    "apps/scirust-hub-worker/src/main.rs",
    '''fn run(args: Args) -> Result<(), WorkerError> {
    let listen: SocketAddr = args
''',
    '''fn install_rustls_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

fn run(args: Args) -> Result<(), WorkerError> {
    // Select the process-level provider explicitly before Rustls configuration
    // is built; this is robust to additive Cargo features enabling both
    // built-in providers elsewhere in the dependency graph.
    install_rustls_crypto_provider();
    let listen: SocketAddr = args
''',
)
replace_once(
    "apps/scirust-hub-worker/src/main.rs",
    '''    #[test]
    fn tls_configuration_requires_cert_and_key_together() {
''',
    '''    #[test]
    fn rustls_crypto_provider_is_installed_explicitly() {
        install_rustls_crypto_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }

    #[test]
    fn tls_configuration_requires_cert_and_key_together() {
''',
)

replace_once(
    "docs/adr/0015-native-server-tls.md",
    '''The TLS daemon path uses `axum-server`'s handle-based graceful shutdown with a
30-second drain bound. The plaintext path retains Axum's existing graceful
shutdown behavior.
''',
    '''The TLS daemon path uses `axum-server`'s handle-based graceful shutdown with a
30-second drain bound. The plaintext path retains Axum's existing graceful
shutdown behavior.

Both binaries explicitly install Rustls' AWS-LC provider at process startup.
This is required because Cargo feature unification can make both AWS-LC and
`ring` available once server TLS and the remote HTTP client coexist; Rustls
refuses to guess between multiple providers. Explicit installation also covers
the daemon's HTTPS worker-client path when the Hub server itself remains HTTP.
''',
)

print("explicit Rustls AWS-LC provider staged")
