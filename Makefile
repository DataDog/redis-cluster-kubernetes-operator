CONTROLLER_GEN_VERSION := v0.18.0
CRD_CONTRACT_SHA256 := a166835578f125ed2ada86b9affaa367d285f59db1fc84e980095e9a666a4c55
BUILD_DIR ?= bin
BINARY ?= $(BUILD_DIR)/redis-cluster-kubernetes-operator

.PHONY: build generate generate-deepcopy verify-crd-contract

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
