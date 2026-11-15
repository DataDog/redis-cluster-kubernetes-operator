CONTROLLER_GEN_VERSION := v0.18.0
CRD_CONTRACT_SHA256 := b1480856fe755ed908621af8597de592282bd55624f33495d195bcc8bbfed456
BUILD_DIR ?= bin
BINARY ?= $(BUILD_DIR)/redis-cluster-kubernetes-operator

.PHONY: build generate generate-deepcopy verify-crd-contract verify-license-inventory verify-generated local-up local-down cleanroom-smoke

build:
	mkdir -p "$(BUILD_DIR)"
	go build -o "$(BINARY)" ./cmd

# Pin controller-gen at the command boundary so generation is reproducible
# without requiring a globally installed binary.
generate: generate-deepcopy verify-crd-contract

generate-deepcopy:
	go run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION) object:headerFile=hack/boilerplate.go.txt paths=./apiv1

# The CRD is a compatibility contract imported from the existing operator.
# Do not regenerate it implicitly: controller-gen versions can expand or
# tighten the schema even when the Go JSON fields are unchanged.
verify-crd-contract:
	go run ./internal/tools/crdcontract -path crds/redis-cluster-v1.yaml -sha256 $(CRD_CONTRACT_SHA256)

# Fail when committed generated code differs from what the pinned generators
# produce. CI runs this exact target.
verify-generated: generate-deepcopy
	git diff --exit-code -- apiv1
	@untracked="$$(git status --porcelain --untracked-files=all -- apiv1)"; \
	if [ -n "$$untracked" ]; then echo "untracked generated files:"; echo "$$untracked"; exit 1; fi
	$(MAKE) verify-crd-contract

local-up:
	./scripts/local-kind-up.sh

local-down:
	./scripts/local-kind-down.sh

cleanroom-smoke:
	./ci/run-cleanroom.sh kind smoke

verify-license-inventory:
	ci/verify-license-inventory.sh
