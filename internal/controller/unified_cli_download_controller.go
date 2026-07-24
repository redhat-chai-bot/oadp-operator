package controller

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/go-logr/logr"
	consolev1 "github.com/openshift/api/console/v1"
	routev1 "github.com/openshift/api/route/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const (
	// Unified resource names
	unifiedServerDeploymentName     = "openshift-adp-unified-cli-server"
	unifiedServerServiceName        = "openshift-adp-unified-cli-server"
	unifiedServerServiceAccountName = "openshift-adp-unified-cli-server"
	unifiedServerRouteName          = "unified-cli-server-route"

	// ConsoleCLIDownload CR names — kept identical to the old controllers so
	// existing CRs are adopted in-place rather than duplicated.
	unifiedCLIDownloadName  = "openshift-adp-oadp-cli"
	unifiedVMDPDownloadName = "openshift-adp-oadp-vmdp"

	// Legacy resource names — used during migration cleanup.
	oldCLIDeploymentName     = "openshift-adp-oadp-cli-server"
	oldCLIServiceName        = "openshift-adp-cli-server"
	oldCLIServiceAccountName = "openshift-adp-cli-server"
	oldCLIRouteName          = "oadp-cli-server-route"

	oldVMDPDeploymentName     = "openshift-adp-oadp-vmdp-server"
	oldVMDPServiceName        = "openshift-adp-vmdp-server"
	oldVMDPServiceAccountName = "openshift-adp-vmdp-server"
	oldVMDPRouteName          = "oadp-vmdp-server-route"

	// Common labels
	managedByLabel = "app.kubernetes.io/managed-by"
	operatorName   = "oadp-operator"

	// Default fallback images (used when env vars are unset)
	defaultCLIImage  = "quay.io/konveyor/oadp-cli-binaries"
	defaultVMDPImage = "quay.io/konveyor/oadp-vmdp-binaries"
)

// ---------------------------------------------------------------------------
// UnifiedCLIDownloadSetup — the single Runnable that replaces both
// CLIDownloadSetup and VMDPDownloadSetup.
// ---------------------------------------------------------------------------

// UnifiedCLIDownloadSetup creates a single deployment that serves both OADP
// CLI and VMDP CLI binaries, replacing the previous two-deployment design.
type UnifiedCLIDownloadSetup struct {
	Client            client.Client
	Namespace         string
	OperatorName      string
	OperatorNamespace string
	Log               logr.Logger
}

// Start implements the manager.Runnable interface.
func (u *UnifiedCLIDownloadSetup) Start(ctx context.Context) error {
	u.Log = ctrl.Log.WithName("unified-cli-download-setup")
	u.Log.Info("Starting unified CLI download setup")

	// Resolve images from environment.
	unifiedImage := os.Getenv("RELATED_IMAGE_UNIFIED_CLI_DOWNLOAD")
	cliImage := os.Getenv("RELATED_IMAGE_CONSOLE_CLI_DOWNLOAD")
	if cliImage == "" {
		cliImage = defaultCLIImage
	}
	vmdpImage := os.Getenv("RELATED_IMAGE_VMDP_CLI_DOWNLOAD")
	if vmdpImage == "" {
		vmdpImage = defaultVMDPImage
	}

	if unifiedImage != "" {
		u.Log.Info("Using unified CLI image", "image", unifiedImage)
	} else {
		u.Log.Info("Unified image not set, using init-container fallback",
			"cliImage", cliImage, "vmdpImage", vmdpImage)
	}

	// Get the operator deployment — used as owner reference for GC.
	operatorDeployment := &appsv1.Deployment{}
	if err := u.Client.Get(ctx, types.NamespacedName{
		Name:      u.OperatorName,
		Namespace: u.OperatorNamespace,
	}, operatorDeployment); err != nil {
		u.Log.Error(err, "Failed to get operator deployment")
		return err
	}

	// Step 1: Clean up legacy (split) resources left by prior versions.
	u.cleanupOldResources(ctx)

	// Step 2: Reconcile the unified resources.
	if err := u.reconcileUnifiedResources(ctx, operatorDeployment, unifiedImage, cliImage, vmdpImage); err != nil {
		return err
	}

	u.Log.Info("Unified CLI download setup completed successfully")
	return nil
}

// ---------------------------------------------------------------------------
// Migration: clean up the old split deployments
// ---------------------------------------------------------------------------

// cleanupOldResources deletes the legacy CLI-server and VMDP-server resources
// that were created by the previous two-controller design. Errors are logged
// but not propagated — the old resources may already be absent.
func (u *UnifiedCLIDownloadSetup) cleanupOldResources(ctx context.Context) {
	type namespacedResource struct {
		obj  client.Object
		desc string
	}

	old := []namespacedResource{
		// Old CLI resources
		{&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: oldCLIDeploymentName, Namespace: u.Namespace}}, "old CLI deployment"},
		{&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: oldCLIServiceName, Namespace: u.Namespace}}, "old CLI service"},
		{&routev1.Route{ObjectMeta: metav1.ObjectMeta{Name: oldCLIRouteName, Namespace: u.Namespace}}, "old CLI route"},
		{&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: oldCLIServiceAccountName, Namespace: u.Namespace}}, "old CLI service account"},
		// Old VMDP resources
		{&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: oldVMDPDeploymentName, Namespace: u.Namespace}}, "old VMDP deployment"},
		{&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: oldVMDPServiceName, Namespace: u.Namespace}}, "old VMDP service"},
		{&routev1.Route{ObjectMeta: metav1.ObjectMeta{Name: oldVMDPRouteName, Namespace: u.Namespace}}, "old VMDP route"},
		{&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: oldVMDPServiceAccountName, Namespace: u.Namespace}}, "old VMDP service account"},
	}

	for _, r := range old {
		if err := u.Client.Delete(ctx, r.obj); err != nil {
			if !errors.IsNotFound(err) {
				u.Log.Error(err, "Failed to delete legacy resource", "resource", r.desc)
			}
		} else {
			u.Log.Info("Deleted legacy resource", "resource", r.desc)
		}
	}
}

// ---------------------------------------------------------------------------
// Reconcile: create / update unified resources
// ---------------------------------------------------------------------------

func (u *UnifiedCLIDownloadSetup) reconcileUnifiedResources(
	ctx context.Context,
	operatorDeployment *appsv1.Deployment,
	unifiedImage, cliImage, vmdpImage string,
) error {
	// 1. ServiceAccount
	if err := u.reconcileServiceAccount(ctx, operatorDeployment); err != nil {
		return err
	}

	// 2. Deployment
	if err := u.reconcileDeployment(ctx, operatorDeployment, unifiedImage, cliImage, vmdpImage); err != nil {
		return err
	}

	// 3. Service
	if err := u.reconcileService(ctx, operatorDeployment); err != nil {
		return err
	}

	// 4. Route
	hostname, err := u.reconcileRoute(ctx, operatorDeployment)
	if err != nil {
		return err
	}
	if hostname == "" {
		u.Log.Info("Route hostname not assigned; ConsoleCLIDownload CRs will be created on next reconciliation")
		return nil
	}

	// 5. ConsoleCLIDownload CRs (two — one for OADP, one for VMDP)
	if err := u.reconcileConsoleCLIDownload(ctx, unifiedCLIDownloadName,
		"OADP operator Command Line Interface (CLI)",
		"oadp - OADP operator Command Line Interface (CLI)",
		"Download OADP CLI",
		fmt.Sprintf("https://%s/oadp/", hostname),
	); err != nil {
		return err
	}
	if err := u.reconcileConsoleCLIDownload(ctx, unifiedVMDPDownloadName,
		"OADP VM Data Protection CLI - back up and restore data inside OpenShift Virtualization guest VMs",
		"oadp-vmdp - OADP VM Data Protection CLI",
		"Download OADP VMDP CLI",
		fmt.Sprintf("https://%s/vmdp/", hostname),
	); err != nil {
		return err
	}

	return nil
}

// ---------------------------------------------------------------------------
// Individual resource reconcilers
// ---------------------------------------------------------------------------

func (u *UnifiedCLIDownloadSetup) reconcileServiceAccount(ctx context.Context, owner *appsv1.Deployment) error {
	sa := &corev1.ServiceAccount{}
	err := u.Client.Get(ctx, client.ObjectKey{Name: unifiedServerServiceAccountName, Namespace: u.Namespace}, sa)

	if errors.IsNotFound(err) {
		sa = buildUnifiedServiceAccount(u.Namespace)
		if err := controllerutil.SetOwnerReference(owner, sa, u.Client.Scheme()); err != nil {
			return fmt.Errorf("failed to set owner reference on service account: %w", err)
		}
		if err := u.Client.Create(ctx, sa); err != nil && !errors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create unified service account: %w", err)
		}
		u.Log.Info("Created unified service account")
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get unified service account: %w", err)
	}

	// Reconcile existing SA to desired state.
	desired := buildUnifiedServiceAccount(u.Namespace)
	needsUpdate := false

	if sa.AutomountServiceAccountToken == nil || *sa.AutomountServiceAccountToken != *desired.AutomountServiceAccountToken {
		sa.AutomountServiceAccountToken = desired.AutomountServiceAccountToken
		needsUpdate = true
	}
	if sa.Labels == nil {
		sa.Labels = make(map[string]string)
	}
	for k, v := range desired.Labels {
		if sa.Labels[k] != v {
			sa.Labels[k] = v
			needsUpdate = true
		}
	}

	hasOwnerRef := false
	for _, ref := range sa.OwnerReferences {
		if ref.UID == owner.UID {
			hasOwnerRef = true
			break
		}
	}
	if err := controllerutil.SetOwnerReference(owner, sa, u.Client.Scheme()); err != nil {
		return fmt.Errorf("failed to set owner reference on service account: %w", err)
	}
	if !hasOwnerRef {
		needsUpdate = true
	}

	if needsUpdate {
		if err := u.Client.Update(ctx, sa); err != nil {
			return fmt.Errorf("failed to update unified service account: %w", err)
		}
		u.Log.Info("Updated unified service account")
	}
	return nil
}

func (u *UnifiedCLIDownloadSetup) reconcileDeployment(
	ctx context.Context,
	owner *appsv1.Deployment,
	unifiedImage, cliImage, vmdpImage string,
) error {
	dep := &appsv1.Deployment{}
	err := u.Client.Get(ctx, client.ObjectKey{Name: unifiedServerDeploymentName, Namespace: u.Namespace}, dep)

	if errors.IsNotFound(err) {
		dep = buildUnifiedDeployment(u.Namespace, unifiedImage, cliImage, vmdpImage)
		if err := controllerutil.SetOwnerReference(owner, dep, u.Client.Scheme()); err != nil {
			return fmt.Errorf("failed to set owner reference on deployment: %w", err)
		}
		if err := u.Client.Create(ctx, dep); err != nil && !errors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create unified deployment: %w", err)
		}
		u.Log.Info("Created unified deployment")
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get unified deployment: %w", err)
	}

	// Backfill probes on existing deployments (upgrade path).
	if idx := findContainerIndexByName(dep.Spec.Template.Spec.Containers, "unified-cli-server"); idx != -1 {
		desired := buildUnifiedDeployment(u.Namespace, unifiedImage, cliImage, vmdpImage)
		desiredContainer := desired.Spec.Template.Spec.Containers[0]
		current := &dep.Spec.Template.Spec.Containers[idx]
		needsUpdate := false

		if current.ReadinessProbe == nil && desiredContainer.ReadinessProbe != nil {
			current.ReadinessProbe = desiredContainer.ReadinessProbe
			needsUpdate = true
		}
		if current.LivenessProbe == nil && desiredContainer.LivenessProbe != nil {
			current.LivenessProbe = desiredContainer.LivenessProbe
			needsUpdate = true
		}
		if current.StartupProbe == nil && desiredContainer.StartupProbe != nil {
			current.StartupProbe = desiredContainer.StartupProbe
			needsUpdate = true
		}
		if dep.Spec.Template.Spec.ServiceAccountName != unifiedServerServiceAccountName {
			dep.Spec.Template.Spec.ServiceAccountName = desired.Spec.Template.Spec.ServiceAccountName
			needsUpdate = true
		}
		desiredAutomount := desired.Spec.Template.Spec.AutomountServiceAccountToken
		currentAutomount := dep.Spec.Template.Spec.AutomountServiceAccountToken
		if desiredAutomount != nil && (currentAutomount == nil || *currentAutomount != *desiredAutomount) {
			dep.Spec.Template.Spec.AutomountServiceAccountToken = desiredAutomount
			needsUpdate = true
		}
		if needsUpdate {
			if err := u.Client.Update(ctx, dep); err != nil {
				return fmt.Errorf("failed to update unified deployment: %w", err)
			}
			u.Log.Info("Updated unified deployment with probes / service account")
		}
	}
	return nil
}

func (u *UnifiedCLIDownloadSetup) reconcileService(ctx context.Context, owner *appsv1.Deployment) error {
	svc := &corev1.Service{}
	err := u.Client.Get(ctx, client.ObjectKey{Name: unifiedServerServiceName, Namespace: u.Namespace}, svc)

	if errors.IsNotFound(err) {
		svc = buildUnifiedService(u.Namespace)
		if err := controllerutil.SetOwnerReference(owner, svc, u.Client.Scheme()); err != nil {
			return fmt.Errorf("failed to set owner reference on service: %w", err)
		}
		if err := u.Client.Create(ctx, svc); err != nil && !errors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create unified service: %w", err)
		}
		u.Log.Info("Created unified service")
	} else if err != nil {
		return fmt.Errorf("failed to get unified service: %w", err)
	}
	return nil
}

func (u *UnifiedCLIDownloadSetup) reconcileRoute(ctx context.Context, owner *appsv1.Deployment) (string, error) {
	route := &routev1.Route{}
	err := u.Client.Get(ctx, client.ObjectKey{Name: unifiedServerRouteName, Namespace: u.Namespace}, route)

	if errors.IsNotFound(err) {
		route = buildUnifiedRoute(u.Namespace)
		if err := controllerutil.SetOwnerReference(owner, route, u.Client.Scheme()); err != nil {
			return "", fmt.Errorf("failed to set owner reference on route: %w", err)
		}
		if err := u.Client.Create(ctx, route); err != nil && !errors.IsAlreadyExists(err) {
			return "", fmt.Errorf("failed to create unified route: %w", err)
		}
		u.Log.Info("Created unified route, waiting for hostname assignment")
		time.Sleep(2 * time.Second)
		if err := u.Client.Get(ctx, client.ObjectKey{Name: unifiedServerRouteName, Namespace: u.Namespace}, route); err != nil {
			return "", fmt.Errorf("failed to get route after creation: %w", err)
		}
	} else if err != nil {
		return "", fmt.Errorf("failed to get unified route: %w", err)
	}

	hostname := route.Spec.Host
	if hostname == "" {
		hostname = u.waitForHostname(ctx)
	}
	return hostname, nil
}

// waitForHostname polls the route until a hostname is assigned or retries are
// exhausted. Returns "" if the hostname is never assigned.
func (u *UnifiedCLIDownloadSetup) waitForHostname(ctx context.Context) string {
	u.Log.Info("Route hostname not yet assigned, retrying with backoff")
	maxRetries := 5
	backoff := 2 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ""
		case <-time.After(backoff):
			route := &routev1.Route{}
			if err := u.Client.Get(ctx, client.ObjectKey{
				Name: unifiedServerRouteName, Namespace: u.Namespace,
			}, route); err != nil {
				u.Log.Error(err, "Failed to get route on retry", "attempt", attempt)
				continue
			}
			if route.Spec.Host != "" {
				u.Log.Info("Route hostname assigned", "hostname", route.Spec.Host, "attempt", attempt)
				return route.Spec.Host
			}
			u.Log.Info("Route hostname still not assigned", "attempt", attempt, "maxRetries", maxRetries)
			backoff *= 2
		}
	}
	return ""
}

func (u *UnifiedCLIDownloadSetup) reconcileConsoleCLIDownload(
	ctx context.Context,
	name, description, displayName, linkText, downloadURL string,
) error {
	existing := &consolev1.ConsoleCLIDownload{}
	err := u.Client.Get(ctx, client.ObjectKey{Name: name}, existing)

	desiredSpec := consolev1.ConsoleCLIDownloadSpec{
		Description: description,
		DisplayName: displayName,
		Links: []consolev1.CLIDownloadLink{
			{Href: downloadURL, Text: linkText},
		},
	}

	if errors.IsNotFound(err) {
		cr := &consolev1.ConsoleCLIDownload{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
				Labels: map[string]string{
					managedByLabel:               operatorName,
					"app.kubernetes.io/instance": u.OperatorNamespace,
				},
			},
			Spec: desiredSpec,
		}
		if err := u.Client.Create(ctx, cr); err != nil && !errors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create ConsoleCLIDownload %s: %w", name, err)
		}
		u.Log.Info("Created ConsoleCLIDownload", "name", name, "url", downloadURL)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get ConsoleCLIDownload %s: %w", name, err)
	}

	// Update if needed.
	needsUpdate := false
	if existing.Labels == nil {
		existing.Labels = make(map[string]string)
	}
	if existing.Labels[managedByLabel] != operatorName {
		existing.Labels[managedByLabel] = operatorName
		existing.Labels["app.kubernetes.io/instance"] = u.OperatorNamespace
		needsUpdate = true
	}
	if len(existing.Spec.Links) == 0 || existing.Spec.Links[0].Href != downloadURL {
		existing.Spec = desiredSpec
		needsUpdate = true
	}
	if needsUpdate {
		if err := u.Client.Update(ctx, existing); err != nil {
			return fmt.Errorf("failed to update ConsoleCLIDownload %s: %w", name, err)
		}
		u.Log.Info("Updated ConsoleCLIDownload", "name", name, "url", downloadURL)
	} else {
		u.Log.Info("ConsoleCLIDownload already up-to-date", "name", name, "url", downloadURL)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Builder helpers
// ---------------------------------------------------------------------------

func buildUnifiedDeployment(namespace, unifiedImage, cliImage, vmdpImage string) *appsv1.Deployment {
	replicas := int32(1)
	runAsNonRoot := true
	allowPrivilegeEscalation := false
	automountServiceAccountToken := false

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      unifiedServerDeploymentName,
			Namespace: namespace,
			Labels: map[string]string{
				"app":          "oadp-unified-cli",
				managedByLabel: operatorName,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "oadp-unified-cli"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "oadp-unified-cli"},
				},
				Spec: corev1.PodSpec{
					ServiceAccountName:           unifiedServerServiceAccountName,
					AutomountServiceAccountToken: &automountServiceAccountToken,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: &runAsNonRoot,
					},
					TerminationGracePeriodSeconds: int64Ptr(10),
				},
			},
		},
	}

	resources := corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("64Mi"),
		},
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("50m"),
			corev1.ResourceMemory: resource.MustParse("32Mi"),
		},
	}

	startupProbe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{Path: "/", Port: intstr.FromString("http")},
		},
		InitialDelaySeconds: 5,
		PeriodSeconds:       5,
		FailureThreshold:    12,
	}
	readinessProbe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{Path: "/", Port: intstr.FromString("http")},
		},
		InitialDelaySeconds: 5,
		PeriodSeconds:       10,
	}
	livenessProbe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{Path: "/", Port: intstr.FromString("http")},
		},
		InitialDelaySeconds: 15,
		PeriodSeconds:       20,
	}

	if unifiedImage != "" {
		// ── Unified image mode ──────────────────────────────────────────
		// Single container from the pre-built unified image that already
		// has both sets of binaries baked in at /downloads/oadp/ and
		// /downloads/vmdp/.
		readOnlyRootFilesystem := true
		dep.Spec.Template.Spec.Containers = []corev1.Container{
			{
				Name:  "unified-cli-server",
				Image: unifiedImage,
				Ports: []corev1.ContainerPort{
					{Name: "http", ContainerPort: 8080, Protocol: corev1.ProtocolTCP},
				},
				Resources: resources,
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: &allowPrivilegeEscalation,
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
					ReadOnlyRootFilesystem:   &readOnlyRootFilesystem,
				},
				StartupProbe:   startupProbe,
				ReadinessProbe: readinessProbe,
				LivenessProbe:  livenessProbe,
			},
		}
	} else {
		// ── Init-container fallback mode ────────────────────────────────
		// When the unified image has not been built, use init containers
		// from the two existing CLI images to populate an emptyDir volume
		// with both sets of binaries. The main container reuses the CLI
		// image and sets WorkingDir to /downloads so the built-in HTTP
		// file server (if it serves from CWD) exposes the combined tree.
		//
		// NOTE: This is a prototype fallback. The init-container copy
		// commands try common content paths. Adjust for your images.
		readOnlyRootFilesystem := false // emptyDir writes needed
		downloadsVolume := corev1.Volume{
			Name:         "downloads",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		}
		downloadsMount := corev1.VolumeMount{Name: "downloads", MountPath: "/downloads"}

		initSecCtx := &corev1.SecurityContext{
			AllowPrivilegeEscalation: &allowPrivilegeEscalation,
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		}

		dep.Spec.Template.Spec.Volumes = []corev1.Volume{downloadsVolume}
		dep.Spec.Template.Spec.InitContainers = []corev1.Container{
			{
				Name:  "prepare-oadp",
				Image: cliImage,
				Command: []string{"sh", "-c",
					"mkdir -p /downloads/oadp && " +
						"cp -a /opt/app-root/src/. /downloads/oadp/ 2>/dev/null; " +
						"cp -a /usr/share/nginx/html/. /downloads/oadp/ 2>/dev/null; " +
						"true"},
				VolumeMounts:    []corev1.VolumeMount{downloadsMount},
				SecurityContext: initSecCtx,
			},
			{
				Name:  "prepare-vmdp",
				Image: vmdpImage,
				Command: []string{"sh", "-c",
					"mkdir -p /downloads/vmdp && " +
						"cp -a /opt/app-root/src/. /downloads/vmdp/ 2>/dev/null; " +
						"cp -a /usr/share/nginx/html/. /downloads/vmdp/ 2>/dev/null; " +
						"true"},
				VolumeMounts:    []corev1.VolumeMount{downloadsMount},
				SecurityContext: initSecCtx,
			},
		}
		dep.Spec.Template.Spec.Containers = []corev1.Container{
			{
				Name:       "unified-cli-server",
				Image:      cliImage,
				WorkingDir: "/downloads",
				Ports: []corev1.ContainerPort{
					{Name: "http", ContainerPort: 8080, Protocol: corev1.ProtocolTCP},
				},
				Resources:    resources,
				VolumeMounts: []corev1.VolumeMount{downloadsMount},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: &allowPrivilegeEscalation,
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
					ReadOnlyRootFilesystem:   &readOnlyRootFilesystem,
				},
				StartupProbe:   startupProbe,
				ReadinessProbe: readinessProbe,
				LivenessProbe:  livenessProbe,
			},
		}
	}

	return dep
}

func buildUnifiedServiceAccount(namespace string) *corev1.ServiceAccount {
	automountServiceAccountToken := false
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      unifiedServerServiceAccountName,
			Namespace: namespace,
			Labels: map[string]string{
				"app":          "oadp-unified-cli",
				managedByLabel: operatorName,
			},
		},
		AutomountServiceAccountToken: &automountServiceAccountToken,
	}
}

func buildUnifiedService(namespace string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      unifiedServerServiceName,
			Namespace: namespace,
			Labels: map[string]string{
				"app":          "oadp-unified-cli",
				managedByLabel: operatorName,
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "oadp-unified-cli"},
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       80,
					TargetPort: intstr.FromInt(8080),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			Type: corev1.ServiceTypeClusterIP,
		},
	}
}

func buildUnifiedRoute(namespace string) *routev1.Route {
	return &routev1.Route{
		ObjectMeta: metav1.ObjectMeta{
			Name:      unifiedServerRouteName,
			Namespace: namespace,
			Labels: map[string]string{
				"app":          "oadp-unified-cli",
				managedByLabel: operatorName,
			},
		},
		Spec: routev1.RouteSpec{
			To: routev1.RouteTargetReference{
				Kind: "Service",
				Name: unifiedServerServiceName,
			},
			Port: &routev1.RoutePort{
				TargetPort: intstr.FromString("http"),
			},
			TLS: &routev1.TLSConfig{
				Termination:                   routev1.TLSTerminationEdge,
				InsecureEdgeTerminationPolicy: routev1.InsecureEdgeTerminationPolicyRedirect,
			},
		},
	}
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

func int64Ptr(i int64) *int64 {
	return &i
}

// findContainerIndexByName returns the index of the container with the given
// name, or -1 if no such container exists.
func findContainerIndexByName(containers []corev1.Container, name string) int {
	for i := range containers {
		if containers[i].Name == name {
			return i
		}
	}
	return -1
}
