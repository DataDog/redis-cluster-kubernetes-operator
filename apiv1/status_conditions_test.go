package v1

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

func TestSetConditionPreservesLastTransitionTimeUntilStatusChanges(t *testing.T) {
	status := &RedisClusterStatus{}
	status.SetCondition(RedisClusterConditionReady, metav1.ConditionFalse, "Creating", "waiting for pods", 7)

	if len(status.Conditions) != 1 {
		t.Fatalf("conditions length = %d, want 1", len(status.Conditions))
	}
	firstTransition := status.Conditions[0].LastTransitionTime
	status.SetCondition(RedisClusterConditionReady, metav1.ConditionFalse, "Reconciling", "waiting for slots", 8)

	condition := status.Conditions[0]
	if condition.LastTransitionTime != firstTransition {
		t.Fatalf("LastTransitionTime changed without a status transition: got %v, want %v", condition.LastTransitionTime, firstTransition)
	}
	if condition.Reason != "Reconciling" || condition.Message != "waiting for slots" || condition.ObservedGeneration != 8 {
		t.Fatalf("condition details were not refreshed: %#v", condition)
	}

	oldTransition := metav1.NewTime(time.Unix(1, 0))
	status.Conditions[0].LastTransitionTime = oldTransition
	status.SetCondition(RedisClusterConditionReady, metav1.ConditionTrue, "Available", "cluster is ready", 8)
	if status.Conditions[0].LastTransitionTime == oldTransition {
		t.Fatal("LastTransitionTime was not refreshed when status changed")
	}
}

func TestRedisClusterStatusDeepCopyCopiesConditions(t *testing.T) {
	original := &RedisClusterStatus{Conditions: []metav1.Condition{{Type: string(RedisClusterConditionDegraded), Status: metav1.ConditionTrue}}}
	copy := original.DeepCopy()
	copy.Conditions[0].Status = metav1.ConditionFalse
	if original.Conditions[0].Status != metav1.ConditionTrue {
		t.Fatal("DeepCopy shared the top-level conditions slice")
	}
}

func TestExistingResourceWithoutConditionsDeserializesUnchanged(t *testing.T) {
	legacy := []byte(`{
		"apiVersion":"cache.datadoghq.com/v1",
		"kind":"RedisCluster",
		"metadata":{"name":"legacy"},
		"spec":{"primaries":3,"replicas":1,"restoreFrom":"","backupEnabled":true,"backupFrequency":3600,"backupRetention":5,"statefulsetTemplate":{}},
		"status":{"nodes":[],"statefulset_ready":true,"last_backup_time":123,"backup_restored":false,"bad_primaries_reports":0}
	}`)

	var cluster RedisCluster
	if err := json.Unmarshal(legacy, &cluster); err != nil {
		t.Fatalf("unmarshal legacy resource: %v", err)
	}
	if cluster.Spec.BackupRetention != 5 || !cluster.Status.StatefulSetReady || cluster.Status.Conditions != nil {
		t.Fatalf("legacy resource changed during decode: %#v", cluster)
	}
	roundTrip, err := json.Marshal(&cluster)
	if err != nil {
		t.Fatalf("marshal legacy resource: %v", err)
	}
	if strings.Contains(string(roundTrip), `"conditions"`) {
		t.Fatalf("omitted legacy conditions became present: %s", roundTrip)
	}
}

func TestCRDSchemaMatchesTopLevelGoFieldsAndConditionContract(t *testing.T) {
	data, err := os.ReadFile("../crds/redis-cluster-v1.yaml")
	if err != nil {
		t.Fatalf("read CRD: %v", err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatalf("decode CRD: %v", err)
	}
	if crd.Spec.Group != GroupVersion.Group || len(crd.Spec.Versions) != 1 || crd.Spec.Versions[0].Name != GroupVersion.Version {
		t.Fatalf("CRD group/version = %s/%v, want %s/%s", crd.Spec.Group, crd.Spec.Versions, GroupVersion.Group, GroupVersion.Version)
	}

	root := crd.Spec.Versions[0].Schema.OpenAPIV3Schema
	assertSchemaFieldsMatchGoType(t, root.Properties["spec"], reflect.TypeOf(RedisClusterSpec{}))
	statusSchema := root.Properties["status"]
	assertSchemaFieldsMatchGoType(t, statusSchema, reflect.TypeOf(RedisClusterStatus{}))

	backupRetention := root.Properties["spec"].Properties["backupRetention"]
	if !strings.Contains(strings.ToLower(backupRetention.Description), "deprecated") || !strings.Contains(strings.ToLower(backupRetention.Description), "unsupported") {
		t.Fatalf("backupRetention description does not mark it deprecated and unsupported: %q", backupRetention.Description)
	}

	conditions := statusSchema.Properties["conditions"]
	if conditions.XListType == nil || *conditions.XListType != "map" || len(conditions.XListMapKeys) != 1 || conditions.XListMapKeys[0] != "type" {
		t.Fatalf("conditions is not a map list keyed by type: %#v", conditions)
	}
	item := conditions.Items.Schema
	wantRequired := []string{"lastTransitionTime", "message", "observedGeneration", "reason", "status", "type"}
	if !reflect.DeepEqual(item.Required, wantRequired) {
		t.Fatalf("condition required fields = %v, want %v", item.Required, wantRequired)
	}
	wantTypes := []apiextensionsv1.JSON{{Raw: []byte(`"Ready"`)}, {Raw: []byte(`"Progressing"`)}, {Raw: []byte(`"Degraded"`)}}
	if !reflect.DeepEqual(item.Properties["type"].Enum, wantTypes) {
		t.Fatalf("condition types = %v, want Ready, Progressing, Degraded", item.Properties["type"].Enum)
	}
}

func assertSchemaFieldsMatchGoType(t *testing.T, schema apiextensionsv1.JSONSchemaProps, typ reflect.Type) {
	t.Helper()
	for i := 0; i < typ.NumField(); i++ {
		jsonName := strings.Split(typ.Field(i).Tag.Get("json"), ",")[0]
		if jsonName == "" || jsonName == "-" {
			continue
		}
		if _, ok := schema.Properties[jsonName]; !ok {
			t.Errorf("CRD schema is missing %s.%s JSON field %q", typ.Name(), typ.Field(i).Name, jsonName)
		}
	}
}
