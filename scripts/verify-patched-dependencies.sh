#!/usr/bin/env bash
set -euo pipefail

repository_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
verification_root=$(mktemp -d "${TMPDIR:-/tmp}/paseo-dependency-verification.XXXXXX")
trap 'rm -rf "$verification_root"' EXIT

bandit_version=1.12.3
bandit_commit=6e2ab9cce4869759809da0a2bce6e6e081cf80ae
bandit_hex_sha256=a253ec03f391755b2126e4181ee2fee05c75b712b407aa399de1831b5088c58e
bandit_source_sha256=832c4e72096b4629551a704df83f83fde6b027ddea83be5e3002ac613b05032f

thousand_island_version=1.5.0
thousand_island_commit=62223053915edcc3beaa6ed8e43e3bfc1217fd7c
thousand_island_hex_sha256=708923d40523e43cf99041ab37a0d4b0ec426ac6438fa3716ab23d919eaeb412
thousand_island_source_sha256=5efc5685a7e9bc98568707a1ffe2065d413ba907e63e14fe790705e8ce4302a0

include_slow=false
case "${1:-}" in
  "") ;;
  --include-slow) include_slow=true ;;
  *) echo "usage: $0 [--include-slow]" >&2; exit 2 ;;
esac

verify_sha256() {
  local path=$1
  local expected=$2
  printf '%s  %s\n' "$expected" "$path" | shasum -a 256 -c -
}

unpack_hex_package() {
  local package=$1
  local version=$2
  local expected_sha=$3
  local output=$4
  local archive="$verification_root/${package}-${version}.tar"
  local envelope="$verification_root/${package}-hex-envelope"

  curl -fsSL "https://repo.hex.pm/tarballs/${package}-${version}.tar" -o "$archive"
  verify_sha256 "$archive" "$expected_sha"
  mkdir -p "$envelope" "$output"
  tar -xf "$archive" -C "$envelope"
  tar -xzf "$envelope/contents.tar.gz" -C "$output"
  cp "$envelope/metadata.config" "$output/hex_metadata.config"
}

unpack_source() {
  local repository=$1
  local commit=$2
  local expected_sha=$3
  local output=$4
  local archive="$verification_root/${repository}-${commit}.tar.gz"

  curl -fsSL "https://github.com/mtrudel/${repository}/archive/${commit}.tar.gz" -o "$archive"
  verify_sha256 "$archive" "$expected_sha"
  mkdir -p "$output"
  tar -xzf "$archive" --strip-components=1 -C "$output"
}

apply_bandit_patch() {
  local output=$1
  for path in \
    lib/bandit.ex \
    lib/bandit/extractor.ex \
    lib/bandit/websocket/connection.ex \
    lib/bandit/websocket/handler.ex
  do
    cp "$repository_root/third_party_bandit/$path" "$output/$path"
  done
}

apply_thousand_island_patch() {
  local output=$1
  cp \
    "$repository_root/third_party_thousand_island/lib/thousand_island/handler.ex" \
    "$output/lib/thousand_island/handler.ex"
}

bandit_hex="$verification_root/bandit-hex"
thousand_island_hex="$verification_root/thousand-island-hex"
unpack_hex_package bandit "$bandit_version" "$bandit_hex_sha256" "$bandit_hex"
unpack_hex_package thousand_island "$thousand_island_version" "$thousand_island_hex_sha256" "$thousand_island_hex"
apply_bandit_patch "$bandit_hex"
apply_thousand_island_patch "$thousand_island_hex"

diff -ru --exclude=.hex --exclude=PASEO_PATCH.md "$bandit_hex" "$repository_root/third_party_bandit"
diff -ru --exclude=.hex --exclude=PASEO_PATCH.md "$thousand_island_hex" "$repository_root/third_party_thousand_island"
echo "Vendored dependencies match the pinned Hex packages plus their declared patch files."

bandit_source="$verification_root/bandit-source"
thousand_island_source="$verification_root/thousand-island-source"
unpack_source bandit "$bandit_commit" "$bandit_source_sha256" "$bandit_source"
unpack_source thousand_island "$thousand_island_commit" "$thousand_island_source_sha256" "$thousand_island_source"
cp "$repository_root/.tool-versions" "$bandit_source/.tool-versions"
cp "$repository_root/.tool-versions" "$thousand_island_source/.tool-versions"
apply_bandit_patch "$bandit_source"
apply_thousand_island_patch "$thousand_island_source"

echo "Running Thousand Island ${thousand_island_version} (${thousand_island_commit}) suite."
(cd "$thousand_island_source" && mix deps.get && mix test)

cp -R "$thousand_island_source" "$bandit_source/patched_thousand_island"
BANDIT_MIX_FILE="$bandit_source/mix.exs" elixir -e '
  path = System.fetch_env!("BANDIT_MIX_FILE")
  source = File.read!(path)
  expected = ~s({:thousand_island, "~> 1.5"})
  replacement = ~s({:thousand_island, path: "patched_thousand_island", override: true})

  if source == String.replace(source, expected, replacement) do
    raise "Bandit dependency declaration changed; update the verifier explicitly"
  end

  File.write!(path, String.replace(source, expected, replacement))
'

echo "Running Bandit ${bandit_version} (${bandit_commit}) suite."
if $include_slow; then
  (cd "$bandit_source" && mix deps.get && mix test --include slow)
else
  (cd "$bandit_source" && mix deps.get && mix test)
fi
