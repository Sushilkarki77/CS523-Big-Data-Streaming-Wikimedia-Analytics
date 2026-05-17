# Shared helpers for Wiki Pulse shell scripts.
# shellcheck shell=bash

# Git Bash on Windows rewrites Unix paths passed to docker; no-op on macOS/Linux.
wiki_pulse_platform_init() {
  case "$(uname -s 2>/dev/null || echo unknown)" in
    MINGW* | MSYS* | CYGWIN*)
      export MSYS_NO_PATHCONV=1
      export MSYS2_ARG_CONV_EXCL='*'
      ;;
  esac
}

wiki_pulse_require_container() {
  local name="$1"
  if ! docker inspect "$name" >/dev/null 2>&1; then
    echo "ERROR: container '${name}' not found. Start the course Docker stack first." >&2
    exit 1
  fi
}

# First attached network (stable when a container has multiple networks on Docker Desktop).
wiki_pulse_docker_network() {
  local container="$1"
  docker inspect "$container" \
    --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{"\n"}}{{end}}' 2>/dev/null \
    | head -n1
}
