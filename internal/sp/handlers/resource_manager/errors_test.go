package resource_manager

import (
	server "github.com/dcm-project/control-plane/internal/sp/api/resource_manager"
	"github.com/dcm-project/control-plane/internal/sp/service"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("handleDeleteInstanceError", func() {
	It("no longer has a dedicated 422 branch: a ProvisioningError falls to the generic 500 default (REQ-DEL-02)", func() {
		// DeleteInstance no longer returns ProvisioningError for a publish
		// failure (that's now retried by the cleanup scheduler instead of
		// failing the API call), so handleDeleteInstanceError has nothing
		// mapping ErrCodeProvisioningError to 422 anymore; any caller that
		// still passes one through falls to the same default as any other
		// unrecognized code.
		resp := handleDeleteInstanceError(service.NewProvisioningError("failed to publish delete for instance x: nats unavailable"))

		defResp, ok := resp.(server.DeleteInstancedefaultApplicationProblemPlusJSONResponse)
		Expect(ok).To(BeTrue())
		Expect(defResp.StatusCode).To(Equal(500))
	})

	It("still maps unrecognized errors to the generic 500 default", func() {
		resp := handleDeleteInstanceError(service.NewInternalError("boom"))

		defResp, ok := resp.(server.DeleteInstancedefaultApplicationProblemPlusJSONResponse)
		Expect(ok).To(BeTrue())
		Expect(defResp.StatusCode).To(Equal(500))
	})
})
