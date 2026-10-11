CONTROLLER_GEN_VERSION := v0.18.0
CRD_CONTRACT_SHA256 := e1a60deefcf83573e7f2b80adbea894750d6006b782a1198e423cdb06cd66101
BUILD_DIR ?= bin
BINARY ?= $(BUILD_DIR)/redis-cluster-kubernetes-operator

.PHONY: build generate generate-deepcopy verify-crd-contract verify-license-inventory local-up local-down cleanroom-smoke

build:
	mkdir -p "$(BUILD_DIR)"
	go build -o "$(BINARY)" ./cmd

# Pin controller-gen at the command boundary so generation is reproducible
# without requiring a globally installed binary.
generate: generate-deepcopy verify-crd-contract

generate-deepcopy:
	go run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION) object paths=./apiv1

# The CRD is a compatibility contract imported from the existing operator.
# Do not regenerate it implicitly: controller-gen versions can expand or
# tighten the schema even when the Go JSON fields are unchanged.
verify-crd-contract:
	go run ./internal/tools/crdcontract -path crds/redis-cluster-v1.yaml -sha256 $(CRD_CONTRACT_SHA256)

local-up:
	./scripts/local-kind-up.sh

local-down:
	./scripts/local-kind-down.sh

cleanroom-smoke:
	./ci/run-cleanroom.sh kind smoke

verify-license-inventory:
	ci/verify-license-inventory.sh
