# Maintainer: Roman Kivalin <roman@shl.dev>
pkgname=ferrometer
pkgver=0.6.0
pkgrel=1
pkgdesc='Lightweight telemetry collector'
arch=('x86_64' 'aarch64')
url='https://github.com/rkivalin/ferrometer'
license=('MIT')
depends=('systemd-libs')
makedepends=('rustup' 'protobuf' 'pkgconf')
backup=('etc/ferrometer/config.toml')
options=(!lto !debug)

prepare() {
  cd "$startdir"
  export RUSTUP_TOOLCHAIN=stable
  cargo fetch --locked --target "$( rustc -vV | sed -n 's/host: //p' )"
}

build() {
  cd "$startdir"
  export RUSTUP_TOOLCHAIN=stable
  export CARGO_TARGET_DIR=target
  cargo build --frozen --release
}

check() {
  cd "$startdir"
  export RUSTUP_TOOLCHAIN=stable
  export CARGO_TARGET_DIR=target
  cargo test --frozen --release
}

package() {
  cd "$startdir"

  install -Dm755 "target/release/$pkgname" "$pkgdir/usr/bin/$pkgname"
  install -Dm644 examples/ferrometer.service "$pkgdir/usr/lib/systemd/system/ferrometer.service"
  install -Dm644 examples/config.toml "$pkgdir/etc/ferrometer/config.toml"
  install -Dm644 LICENSE "$pkgdir/usr/share/licenses/$pkgname/LICENSE"
}
