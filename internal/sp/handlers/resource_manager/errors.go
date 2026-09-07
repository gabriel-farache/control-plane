package resource_manager

import (
	"context"
	"errors"

	server "github.com/dcm-project/control-plane/internal/sp/api/resource_manager"
	"github.com/dcm-project/control-plane/internal/sp/logging"
	"github.com/dcm-project/control-plane/internal/sp/service"
)

// logServiceError logs at Warn level for client errors (4xx) and Error level
// for internal failures (5xx), so log severity matches HTTP response semantics.
func logServiceError(ctx context.Context, msg string, err error, attrs ...any) {
	log := logging.FromContext(ctx)
	args := append([]any{"error", err}, attrs...)
	var svcErr *service.ServiceError
	if service.IsClientError(err, &svcErr) {
		log.Warn(msg, args...)
	} else {
		log.Error(msg, args...)
	}
}

// newError creates an RFC 7807 compliant error response.
func newError(errType, title, detail string, status int) server.Error {
	return server.Error{
		Type:   errType,
		Title:  title,
		Detail: &detail,
		Status: &status,
	}
}

// internalErrorDetail is the fixed 5xx response detail used across this
// file. The real error (which may contain DB/NATS/internal detail) is
// always logged server-side by logServiceError before these functions are
// called, so the client-facing body never echoes it back (F37/P: 5xx bodies
// must not leak internal error strings).
const internalErrorDetail = "an internal error occurred"

// handleListInstancesError converts a service error to a ListInstances response.
func handleListInstancesError(err error) server.ListInstancesResponseObject {
	var svcErr *service.ServiceError
	if errors.As(err, &svcErr) && svcErr.Code == service.ErrCodeValidation {
		return server.ListInstances400ApplicationProblemPlusJSONResponse(newError("validation-error", "Invalid request", svcErr.Message, 400))
	}
	return server.ListInstancesdefaultApplicationProblemPlusJSONResponse{
		Body:       newError("list-error", "Failed to list instances", internalErrorDetail, 500),
		StatusCode: 500,
	}
}

// handleCreateInstanceError converts a service error to a CreateInstance response.
func handleCreateInstanceError(err error) server.CreateInstanceResponseObject {
	var svcErr *service.ServiceError
	if errors.As(err, &svcErr) {
		switch svcErr.Code {
		case service.ErrCodeValidation:
			return server.CreateInstance400ApplicationProblemPlusJSONResponse(newError("validation-error", "Validation failed", svcErr.Message, 400))
		case service.ErrCodeNotFound:
			return server.CreateInstance404ApplicationProblemPlusJSONResponse(newError("not-found", "Resource not found", svcErr.Message, 404))
		case service.ErrCodeConflict:
			return server.CreateInstance409ApplicationProblemPlusJSONResponse(newError("conflict", "Resource conflict", svcErr.Message, 409))
		case service.ErrCodeProvisioningError:
			return server.CreateInstance422ApplicationProblemPlusJSONResponse(newError("provisioning-error", "Provisioning error", svcErr.Message, 422))
		case service.ErrCodeInternal:
			return server.CreateInstancedefaultApplicationProblemPlusJSONResponse{
				Body:       newError("internal-error", "Internal error", internalErrorDetail, 500),
				StatusCode: 500,
			}
		case service.ErrCodeUnavailable:
			return server.CreateInstance503ApplicationProblemPlusJSONResponse(newError("unavailable", "Service unavailable", "service temporarily unavailable", 503))
		}
	}
	return server.CreateInstancedefaultApplicationProblemPlusJSONResponse{
		Body:       newError("create-error", "Failed to create instance", internalErrorDetail, 500),
		StatusCode: 500,
	}
}

// handleGetInstanceError converts a service error to a GetInstance response.
func handleGetInstanceError(err error) server.GetInstanceResponseObject {
	var svcErr *service.ServiceError
	if errors.As(err, &svcErr) {
		switch svcErr.Code {
		case service.ErrCodeValidation:
			return server.GetInstance400ApplicationProblemPlusJSONResponse(newError("validation-error", "Invalid request", svcErr.Message, 400))
		case service.ErrCodeNotFound:
			return server.GetInstance404ApplicationProblemPlusJSONResponse(newError("not-found", "Instance not found", svcErr.Message, 404))
		}
	}
	return server.GetInstancedefaultApplicationProblemPlusJSONResponse{
		Body:       newError("get-error", "Failed to get instance", internalErrorDetail, 500),
		StatusCode: 500,
	}
}

// handleDeleteInstanceError converts a service error to a DeleteInstance response.
func handleDeleteInstanceError(err error) server.DeleteInstanceResponseObject {
	var svcErr *service.ServiceError
	if errors.As(err, &svcErr) {
		switch svcErr.Code {
		case service.ErrCodeValidation:
			return server.DeleteInstance400ApplicationProblemPlusJSONResponse(newError("validation-error", "Invalid request", svcErr.Message, 400))
		case service.ErrCodeNotFound:
			return server.DeleteInstance404ApplicationProblemPlusJSONResponse(newError("not-found", "Instance not found", svcErr.Message, 404))
		}
	}
	return server.DeleteInstancedefaultApplicationProblemPlusJSONResponse{
		Body:       newError("delete-error", "Failed to delete instance", internalErrorDetail, 500),
		StatusCode: 500,
	}
}
