package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	consolev1 "github.com/openshift/api/console/v1"
	routev1 "github.com/openshift/api/route/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// getUnifiedTestScheme registers console/route API groups (needed for
// full reconciliation that includes Route and ConsoleCLIDownload steps).
func getUnifiedTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	if err := consolev1.AddToScheme(scheme.Scheme); err != nil {
		t.Fatalf("failed to add consolev1 to scheme: %v", err)
	}
	if err := routev1.AddToScheme(scheme.Scheme); err != nil {
		t.Fatalf("failed to add routev1 to scheme: %v", err)
	}
	return scheme.Scheme
}

// newPartialTestScheme registers only corev1 and appsv1 — enough for
// ServiceAccount/Deployment steps but not Route/ConsoleCLIDownload.
func newPartialTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func newTestOperatorDeployment() *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "oadp-operator",
			Namespace: "openshift-adp",
			UID:       types.UID("test-operator-uid"),
		},
	}
}

// newTestUnifiedRoute returns a unified route with a hostname already assigned.
func newTestUnifiedRoute(namespace string) *routev1.Route {
	route := buildUnifiedRoute(namespace)
	route.Spec.Host = "unified-cli.example.com"
	return route
}

// ---------------------------------------------------------------------------
// Deployment builder tests
// ---------------------------------------------------------------------------

func TestBuildUnifiedDeployment_UnifiedImage(t *testing.T) {
	dep := buildUnifiedDeployment("openshift-adp", "quay.io/konveyor/unified:latest", "", "")

	if dep.Name != unifiedServerDeploymentName {
		t.Errorf("expected deployment name %q, got %q", unifiedServerDeploymentName, dep.Name)
	}
	if len(dep.Spec.Template.Spec.InitContainers) != 0 {
		t.Errorf("expected no init containers with unified image, got %d", len(dep.Spec.Template.Spec.InitContainers))
	}
	if len(dep.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(dep.Spec.Template.Spec.Containers))
	}

	c := dep.Spec.Template.Spec.Containers[0]
	if c.Name != "unified-cli-server" {
		t.Errorf("expected container name %q, got %q", "unified-cli-server", c.Name)
	}
	if c.Image != "quay.io/konveyor/unified:latest" {
		t.Errorf("expected image %q, got %q", "quay.io/konveyor/unified:latest", c.Image)
	}
	if c.Ports[0].ContainerPort != 8080 {
		t.Errorf("expected port 8080, got %d", c.Ports[0].ContainerPort)
	}

	// Verify probes
	if c.StartupProbe == nil {
		t.Error("expected StartupProbe to be set")
	}
	if c.ReadinessProbe == nil {
		t.Error("expected ReadinessProbe to be set")
	}
	if c.LivenessProbe == nil {
		t.Error("expected LivenessProbe to be set")
	}

	// Verify security: ReadOnlyRootFilesystem should be true for unified image
	if c.SecurityContext == nil || c.SecurityContext.ReadOnlyRootFilesystem == nil || !*c.SecurityContext.ReadOnlyRootFilesystem {
		t.Error("expected ReadOnlyRootFilesystem=true for unified image mode")
	}

	// Verify resources
	cpuReq := c.Resources.Requests[corev1.ResourceCPU]
	if cpuReq.String() != "50m" {
		t.Errorf("expected CPU request 50m, got %s", cpuReq.String())
	}
	memReq := c.Resources.Requests[corev1.ResourceMemory]
	if memReq.String() != "32Mi" {
		t.Errorf("expected memory request 32Mi, got %s", memReq.String())
	}
}

func TestBuildUnifiedDeployment_InitContainerFallback(t *testing.T) {
	dep := buildUnifiedDeployment("openshift-adp", "", "cli-image:v1", "vmdp-image:v1")

	// Should have init containers for both images
	if len(dep.Spec.Template.Spec.InitContainers) != 2 {
		t.Fatalf("expected 2 init containers, got %d", len(dep.Spec.Template.Spec.InitContainers))
	}
	if dep.Spec.Template.Spec.InitContainers[0].Name != "prepare-oadp" {
		t.Errorf("expected first init container name %q, got %q", "prepare-oadp", dep.Spec.Template.Spec.InitContainers[0].Name)
	}
	if dep.Spec.Template.Spec.InitContainers[0].Image != "cli-image:v1" {
		t.Errorf("expected first init container image %q, got %q", "cli-image:v1", dep.Spec.Template.Spec.InitContainers[0].Image)
	}
	if dep.Spec.Template.Spec.InitContainers[1].Name != "prepare-vmdp" {
		t.Errorf("expected second init container name %q, got %q", "prepare-vmdp", dep.Spec.Template.Spec.InitContainers[1].Name)
	}
	if dep.Spec.Template.Spec.InitContainers[1].Image != "vmdp-image:v1" {
		t.Errorf("expected second init container image %q, got %q", "vmdp-image:v1", dep.Spec.Template.Spec.InitContainers[1].Image)
	}

	// Should have emptyDir volume
	if len(dep.Spec.Template.Spec.Volumes) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(dep.Spec.Template.Spec.Volumes))
	}
	if dep.Spec.Template.Spec.Volumes[0].Name != "downloads" {
		t.Errorf("expected volume name %q, got %q", "downloads", dep.Spec.Template.Spec.Volumes[0].Name)
	}
	if dep.Spec.Template.Spec.Volumes[0].EmptyDir == nil {
		t.Error("expected EmptyDir volume source")
	}

	// Main container should use CLI image with WorkingDir set
	if len(dep.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(dep.Spec.Template.Spec.Containers))
	}
	c := dep.Spec.Template.Spec.Containers[0]
	if c.Image != "cli-image:v1" {
		t.Errorf("expected main container image %q, got %q", "cli-image:v1", c.Image)
	}
	if c.WorkingDir != "/downloads" {
		t.Errorf("expected WorkingDir %q, got %q", "/downloads", c.WorkingDir)
	}

	// Volume mount on main container
	if len(c.VolumeMounts) != 1 || c.VolumeMounts[0].Name != "downloads" {
		t.Error("expected downloads volume mount on main container")
	}
}

func TestBuildUnifiedDeployment_StartupProbe(t *testing.T) {
	dep := buildUnifiedDeployment("openshift-adp", "test-image", "", "")

	c := dep.Spec.Template.Spec.Containers[0]
	if c.StartupProbe == nil {
		t.Fatal("expected StartupProbe to be set")
	}
	if c.StartupProbe.HTTPGet == nil {
		t.Fatal("expected StartupProbe to use HTTPGet")
	}
	if c.StartupProbe.HTTPGet.Path != "/" {
		t.Errorf("expected path \"/\", got %q", c.StartupProbe.HTTPGet.Path)
	}
	if c.StartupProbe.HTTPGet.Port != intstr.FromString("http") {
		t.Errorf("expected port \"http\", got %v", c.StartupProbe.HTTPGet.Port)
	}
	if c.StartupProbe.FailureThreshold != 12 {
		t.Errorf("expected failureThreshold 12, got %d", c.StartupProbe.FailureThreshold)
	}
}

func TestBuildUnifiedDeployment_ServiceAccount(t *testing.T) {
	dep := buildUnifiedDeployment("openshift-adp", "test-image", "", "")
	podSpec := dep.Spec.Template.Spec

	if podSpec.ServiceAccountName != unifiedServerServiceAccountName {
		t.Errorf("expected serviceAccountName %q, got %q", unifiedServerServiceAccountName, podSpec.ServiceAccountName)
	}
	if podSpec.AutomountServiceAccountToken == nil || *podSpec.AutomountServiceAccountToken {
		t.Error("expected AutomountServiceAccountToken to be false")
	}
}

func TestBuildUnifiedServiceAccount(t *testing.T) {
	const ns = "openshift-adp"
	sa := buildUnifiedServiceAccount(ns)

	if sa.Name != unifiedServerServiceAccountName {
		t.Errorf("expected name %q, got %q", unifiedServerServiceAccountName, sa.Name)
	}
	if sa.Namespace != ns {
		t.Errorf("expected namespace %q, got %q", ns, sa.Namespace)
	}
	if sa.AutomountServiceAccountToken == nil || *sa.AutomountServiceAccountToken {
		t.Error("expected AutomountServiceAccountToken to be false")
	}
	if sa.Labels[managedByLabel] != operatorName {
		t.Errorf("expected label %q=%q, got %q", managedByLabel, operatorName, sa.Labels[managedByLabel])
	}
}

// ---------------------------------------------------------------------------
// Migration / cleanup tests
// ---------------------------------------------------------------------------

func TestCleanupOldResources_DeletesLegacyResources(t *testing.T) {
	const ns = "openshift-adp"
	testScheme := getUnifiedTestScheme(t)

	// Seed the fake cluster with all 8 legacy resources.
	oldObjects := []client.Object{
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: oldCLIDeploymentName, Namespace: ns}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: oldCLIServiceName, Namespace: ns}},
		&routev1.Route{ObjectMeta: metav1.ObjectMeta{Name: oldCLIRouteName, Namespace: ns}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: oldCLIServiceAccountName, Namespace: ns}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: oldVMDPDeploymentName, Namespace: ns}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: oldVMDPServiceName, Namespace: ns}},
		&routev1.Route{ObjectMeta: metav1.ObjectMeta{Name: oldVMDPRouteName, Namespace: ns}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: oldVMDPServiceAccountName, Namespace: ns}},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(oldObjects...).Build()
	setup := &UnifiedCLIDownloadSetup{
		Client:    fakeClient,
		Namespace: ns,
		Log:       logr.Discard(),
	}

	setup.cleanupOldResources(context.Background())

	// Verify all legacy resources are gone.
	for _, obj := range oldObjects {
		key := client.ObjectKeyFromObject(obj)
		err := fakeClient.Get(context.Background(), key, obj)
		if err == nil {
			t.Errorf("expected legacy resource %s/%s to be deleted, but it still exists", obj.GetObjectKind().GroupVersionKind().Kind, key.Name)
		}
	}
}

func TestCleanupOldResources_NoErrorWhenAbsent(t *testing.T) {
	const ns = "openshift-adp"
	testScheme := getUnifiedTestScheme(t)

	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).Build()
	setup := &UnifiedCLIDownloadSetup{
		Client:    fakeClient,
		Namespace: ns,
		Log:       logr.Discard(),
	}

	// Should not panic or log errors when old resources don't exist.
	setup.cleanupOldResources(context.Background())
}

// ---------------------------------------------------------------------------
// ConsoleCLIDownload creation tests
// ---------------------------------------------------------------------------

func TestReconcileUnifiedResources_CreatesBothConsoleCLIDownloads(t *testing.T) {
	const ns = "openshift-adp"
	testScheme := getUnifiedTestScheme(t)
	operatorDeploy := newTestOperatorDeployment()

	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithObjects(operatorDeploy, newTestUnifiedRoute(ns)).
		Build()

	setup := &UnifiedCLIDownloadSetup{
		Client:            fakeClient,
		Namespace:         ns,
		OperatorName:      "oadp-operator",
		OperatorNamespace: ns,
		Log:               logr.Discard(),
	}

	err := setup.reconcileUnifiedResources(
		context.Background(), operatorDeploy, "test-image", "", "",
	)
	if err != nil {
		t.Fatalf("reconcileUnifiedResources returned error: %v", err)
	}

	// Verify OADP ConsoleCLIDownload
	oadpCR := &consolev1.ConsoleCLIDownload{}
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Name: unifiedCLIDownloadName}, oadpCR); err != nil {
		t.Fatalf("failed to get OADP ConsoleCLIDownload: %v", err)
	}
	if len(oadpCR.Spec.Links) == 0 {
		t.Fatal("expected OADP ConsoleCLIDownload to have links")
	}
	expectedOADPURL := "https://unified-cli.example.com/oadp/"
	if oadpCR.Spec.Links[0].Href != expectedOADPURL {
		t.Errorf("expected OADP URL %q, got %q", expectedOADPURL, oadpCR.Spec.Links[0].Href)
	}
	if !strings.Contains(oadpCR.Spec.DisplayName, "oadp") {
		t.Errorf("expected OADP display name to contain 'oadp', got %q", oadpCR.Spec.DisplayName)
	}

	// Verify VMDP ConsoleCLIDownload
	vmdpCR := &consolev1.ConsoleCLIDownload{}
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Name: unifiedVMDPDownloadName}, vmdpCR); err != nil {
		t.Fatalf("failed to get VMDP ConsoleCLIDownload: %v", err)
	}
	if len(vmdpCR.Spec.Links) == 0 {
		t.Fatal("expected VMDP ConsoleCLIDownload to have links")
	}
	expectedVMDPURL := "https://unified-cli.example.com/vmdp/"
	if vmdpCR.Spec.Links[0].Href != expectedVMDPURL {
		t.Errorf("expected VMDP URL %q, got %q", expectedVMDPURL, vmdpCR.Spec.Links[0].Href)
	}
	if !strings.Contains(vmdpCR.Spec.DisplayName, "vmdp") {
		t.Errorf("expected VMDP display name to contain 'vmdp', got %q", vmdpCR.Spec.DisplayName)
	}
}

func TestReconcileUnifiedResources_BackfillsMissingProbes(t *testing.T) {
	const ns = "openshift-adp"
	testScheme := getUnifiedTestScheme(t)
	operatorDeploy := newTestOperatorDeployment()

	// Simulate a deployment without probes (upgrade scenario).
	existingDep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: unifiedServerDeploymentName, Namespace: ns},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "unified-cli-server", Image: "old-image"},
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithObjects(operatorDeploy, existingDep, newTestUnifiedRoute(ns)).
		Build()

	setup := &UnifiedCLIDownloadSetup{
		Client:            fakeClient,
		Namespace:         ns,
		OperatorName:      "oadp-operator",
		OperatorNamespace: ns,
		Log:               logr.Discard(),
	}

	err := setup.reconcileUnifiedResources(
		context.Background(), operatorDeploy, "test-image", "", "",
	)
	if err != nil {
		t.Fatalf("reconcileUnifiedResources returned error: %v", err)
	}

	updated := &appsv1.Deployment{}
	if err := fakeClient.Get(context.Background(), client.ObjectKey{
		Name: unifiedServerDeploymentName, Namespace: ns,
	}, updated); err != nil {
		t.Fatalf("failed to get deployment: %v", err)
	}
	c := updated.Spec.Template.Spec.Containers[0]
	if c.StartupProbe == nil {
		t.Error("expected StartupProbe to be backfilled")
	}
	if c.ReadinessProbe == nil {
		t.Error("expected ReadinessProbe to be backfilled")
	}
	if c.LivenessProbe == nil {
		t.Error("expected LivenessProbe to be backfilled")
	}
	// Image should not be overwritten by backfill.
	if c.Image != "old-image" {
		t.Errorf("expected image to remain %q, got %q", "old-image", c.Image)
	}
}

func TestReconcileUnifiedResources_CreatesServiceAccountWhenMissing(t *testing.T) {
	const ns = "openshift-adp"
	testScheme := newPartialTestScheme(t)
	operatorDeploy := newTestOperatorDeployment()
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(operatorDeploy).Build()

	setup := &UnifiedCLIDownloadSetup{
		Client:    fakeClient,
		Namespace: ns,
		Log:       logr.Discard(),
	}

	// reconcileUnifiedResources will fail at the Route step (not registered
	// in this scheme), but ServiceAccount creation will have already run.
	_ = setup.reconcileUnifiedResources(context.Background(), operatorDeploy, "img", "", "")

	got := &corev1.ServiceAccount{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name: unifiedServerServiceAccountName, Namespace: ns,
	}, got); err != nil {
		t.Fatalf("expected service account to be created: %v", err)
	}
	if got.AutomountServiceAccountToken == nil || *got.AutomountServiceAccountToken {
		t.Error("expected AutomountServiceAccountToken=false")
	}
	if got.Labels[managedByLabel] != operatorName {
		t.Errorf("expected managed-by label %q, got %q", operatorName, got.Labels[managedByLabel])
	}
	if len(got.OwnerReferences) == 0 || got.OwnerReferences[0].UID != operatorDeploy.UID {
		t.Error("expected owner reference to be set")
	}
}
