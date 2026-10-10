#!/usr/bin/env bash

local_kind_cluster_identity() {
  local cluster_name="$1"
  local node_name

  node_name="$(kind get nodes --name "$cluster_name" 2>/dev/null | head -n 1)"
  [[ -n "$node_name" ]] || return 1
  docker inspect --format '{{.Id}}' "$node_name" 2>/dev/null
}

local_kind_record_ownership() {
  local cluster_name="$1"
  local ownership_path="$2"
  local cluster_identity

  cluster_identity="$(local_kind_cluster_identity "$cluster_name")" || return 1
  [[ -n "$cluster_identity" ]] || return 1
  mkdir -p "$(dirname "$ownership_path")"
  printf '%s\t%s\n' "$cluster_name" "$cluster_identity" >"$ownership_path"
}

local_kind_is_owned() {
  local cluster_name="$1"
  local ownership_path="$2"
  local recorded_name recorded_identity current_identity

  [[ -f "$ownership_path" ]] || return 1
  IFS=$'\t' read -r recorded_name recorded_identity <"$ownership_path"
  [[ "$recorded_name" == "$cluster_name" && -n "$recorded_identity" ]] || return 1
  current_identity="$(local_kind_cluster_identity "$cluster_name")" || return 1
  [[ "$current_identity" == "$recorded_identity" ]]
}
