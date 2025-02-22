// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/derektruong/fxfer/protoc (interfaces: ConnectionPool,S3API,Client)
//
// Generated by this command:
//
//	mockgen -destination=./mock_protoc.go -package=mock_protoc github.com/derektruong/fxfer/protoc ConnectionPool,S3API,Client
//

// Package mock_protoc is a generated GoMock package.
package mock_protoc

import (
	context "context"
	io "io"
	reflect "reflect"
	time "time"

	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	protoc "github.com/derektruong/fxfer/protoc"
	logr "github.com/go-logr/logr"
	gomock "go.uber.org/mock/gomock"
)

// MockConnectionPool is a mock of ConnectionPool interface.
type MockConnectionPool struct {
	ctrl     *gomock.Controller
	recorder *MockConnectionPoolMockRecorder
	isgomock struct{}
}

// MockConnectionPoolMockRecorder is the mock recorder for MockConnectionPool.
type MockConnectionPoolMockRecorder struct {
	mock *MockConnectionPool
}

// NewMockConnectionPool creates a new mock instance.
func NewMockConnectionPool(ctrl *gomock.Controller) *MockConnectionPool {
	mock := &MockConnectionPool{ctrl: ctrl}
	mock.recorder = &MockConnectionPoolMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockConnectionPool) EXPECT() *MockConnectionPoolMockRecorder {
	return m.recorder
}

// AppendToFile mocks base method.
func (m *MockConnectionPool) AppendToFile(ctx context.Context, filePath string, reader io.Reader, offset int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AppendToFile", ctx, filePath, reader, offset)
	ret0, _ := ret[0].(error)
	return ret0
}

// AppendToFile indicates an expected call of AppendToFile.
func (mr *MockConnectionPoolMockRecorder) AppendToFile(ctx, filePath, reader, offset any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AppendToFile", reflect.TypeOf((*MockConnectionPool)(nil).AppendToFile), ctx, filePath, reader, offset)
}

// Close mocks base method.
func (m *MockConnectionPool) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockConnectionPoolMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockConnectionPool)(nil).Close))
}

// CreateOrOverwriteFile mocks base method.
func (m *MockConnectionPool) CreateOrOverwriteFile(ctx context.Context, filePath string, reader io.Reader) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateOrOverwriteFile", ctx, filePath, reader)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateOrOverwriteFile indicates an expected call of CreateOrOverwriteFile.
func (mr *MockConnectionPoolMockRecorder) CreateOrOverwriteFile(ctx, filePath, reader any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateOrOverwriteFile", reflect.TypeOf((*MockConnectionPool)(nil).CreateOrOverwriteFile), ctx, filePath, reader)
}

// DeleteFile mocks base method.
func (m *MockConnectionPool) DeleteFile(ctx context.Context, filePath string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteFile", ctx, filePath)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteFile indicates an expected call of DeleteFile.
func (mr *MockConnectionPoolMockRecorder) DeleteFile(ctx, filePath any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteFile", reflect.TypeOf((*MockConnectionPool)(nil).DeleteFile), ctx, filePath)
}

// GetFileSizeAndModTime mocks base method.
func (m *MockConnectionPool) GetFileSizeAndModTime(ctx context.Context, filePath string) (int64, time.Time, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetFileSizeAndModTime", ctx, filePath)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(time.Time)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetFileSizeAndModTime indicates an expected call of GetFileSizeAndModTime.
func (mr *MockConnectionPoolMockRecorder) GetFileSizeAndModTime(ctx, filePath any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetFileSizeAndModTime", reflect.TypeOf((*MockConnectionPool)(nil).GetFileSizeAndModTime), ctx, filePath)
}

// InitializeIdleConnection mocks base method.
func (m *MockConnectionPool) InitializeIdleConnection(credential any) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "InitializeIdleConnection", credential)
	ret0, _ := ret[0].(error)
	return ret0
}

// InitializeIdleConnection indicates an expected call of InitializeIdleConnection.
func (mr *MockConnectionPoolMockRecorder) InitializeIdleConnection(credential any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "InitializeIdleConnection", reflect.TypeOf((*MockConnectionPool)(nil).InitializeIdleConnection), credential)
}

// MakeDirectoryAll mocks base method.
func (m *MockConnectionPool) MakeDirectoryAll(ctx context.Context, dirPath string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MakeDirectoryAll", ctx, dirPath)
	ret0, _ := ret[0].(error)
	return ret0
}

// MakeDirectoryAll indicates an expected call of MakeDirectoryAll.
func (mr *MockConnectionPoolMockRecorder) MakeDirectoryAll(ctx, dirPath any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MakeDirectoryAll", reflect.TypeOf((*MockConnectionPool)(nil).MakeDirectoryAll), ctx, dirPath)
}

// RetrieveFileFromOffset mocks base method.
func (m *MockConnectionPool) RetrieveFileFromOffset(ctx context.Context, filePath string, offset int64) (io.ReadCloser, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RetrieveFileFromOffset", ctx, filePath, offset)
	ret0, _ := ret[0].(io.ReadCloser)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RetrieveFileFromOffset indicates an expected call of RetrieveFileFromOffset.
func (mr *MockConnectionPoolMockRecorder) RetrieveFileFromOffset(ctx, filePath, offset any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RetrieveFileFromOffset", reflect.TypeOf((*MockConnectionPool)(nil).RetrieveFileFromOffset), ctx, filePath, offset)
}

// MockS3API is a mock of S3API interface.
type MockS3API struct {
	ctrl     *gomock.Controller
	recorder *MockS3APIMockRecorder
	isgomock struct{}
}

// MockS3APIMockRecorder is the mock recorder for MockS3API.
type MockS3APIMockRecorder struct {
	mock *MockS3API
}

// NewMockS3API creates a new mock instance.
func NewMockS3API(ctrl *gomock.Controller) *MockS3API {
	mock := &MockS3API{ctrl: ctrl}
	mock.recorder = &MockS3APIMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockS3API) EXPECT() *MockS3APIMockRecorder {
	return m.recorder
}

// AbortMultipartUpload mocks base method.
func (m *MockS3API) AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput, opt ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	m.ctrl.T.Helper()
	varargs := []any{ctx, input}
	for _, a := range opt {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "AbortMultipartUpload", varargs...)
	ret0, _ := ret[0].(*s3.AbortMultipartUploadOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AbortMultipartUpload indicates an expected call of AbortMultipartUpload.
func (mr *MockS3APIMockRecorder) AbortMultipartUpload(ctx, input any, opt ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, input}, opt...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AbortMultipartUpload", reflect.TypeOf((*MockS3API)(nil).AbortMultipartUpload), varargs...)
}

// CompleteMultipartUpload mocks base method.
func (m *MockS3API) CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput, opt ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	m.ctrl.T.Helper()
	varargs := []any{ctx, input}
	for _, a := range opt {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "CompleteMultipartUpload", varargs...)
	ret0, _ := ret[0].(*s3.CompleteMultipartUploadOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CompleteMultipartUpload indicates an expected call of CompleteMultipartUpload.
func (mr *MockS3APIMockRecorder) CompleteMultipartUpload(ctx, input any, opt ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, input}, opt...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CompleteMultipartUpload", reflect.TypeOf((*MockS3API)(nil).CompleteMultipartUpload), varargs...)
}

// CreateMultipartUpload mocks base method.
func (m *MockS3API) CreateMultipartUpload(ctx context.Context, input *s3.CreateMultipartUploadInput, opt ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	m.ctrl.T.Helper()
	varargs := []any{ctx, input}
	for _, a := range opt {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "CreateMultipartUpload", varargs...)
	ret0, _ := ret[0].(*s3.CreateMultipartUploadOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreateMultipartUpload indicates an expected call of CreateMultipartUpload.
func (mr *MockS3APIMockRecorder) CreateMultipartUpload(ctx, input any, opt ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, input}, opt...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateMultipartUpload", reflect.TypeOf((*MockS3API)(nil).CreateMultipartUpload), varargs...)
}

// DeleteObject mocks base method.
func (m *MockS3API) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput, opt ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	m.ctrl.T.Helper()
	varargs := []any{ctx, input}
	for _, a := range opt {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "DeleteObject", varargs...)
	ret0, _ := ret[0].(*s3.DeleteObjectOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteObject indicates an expected call of DeleteObject.
func (mr *MockS3APIMockRecorder) DeleteObject(ctx, input any, opt ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, input}, opt...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteObject", reflect.TypeOf((*MockS3API)(nil).DeleteObject), varargs...)
}

// DeleteObjects mocks base method.
func (m *MockS3API) DeleteObjects(ctx context.Context, input *s3.DeleteObjectsInput, opt ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	m.ctrl.T.Helper()
	varargs := []any{ctx, input}
	for _, a := range opt {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "DeleteObjects", varargs...)
	ret0, _ := ret[0].(*s3.DeleteObjectsOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteObjects indicates an expected call of DeleteObjects.
func (mr *MockS3APIMockRecorder) DeleteObjects(ctx, input any, opt ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, input}, opt...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteObjects", reflect.TypeOf((*MockS3API)(nil).DeleteObjects), varargs...)
}

// GetObject mocks base method.
func (m *MockS3API) GetObject(ctx context.Context, input *s3.GetObjectInput, opt ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.ctrl.T.Helper()
	varargs := []any{ctx, input}
	for _, a := range opt {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetObject", varargs...)
	ret0, _ := ret[0].(*s3.GetObjectOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetObject indicates an expected call of GetObject.
func (mr *MockS3APIMockRecorder) GetObject(ctx, input any, opt ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, input}, opt...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetObject", reflect.TypeOf((*MockS3API)(nil).GetObject), varargs...)
}

// HeadObject mocks base method.
func (m *MockS3API) HeadObject(ctx context.Context, input *s3.HeadObjectInput, opt ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	m.ctrl.T.Helper()
	varargs := []any{ctx, input}
	for _, a := range opt {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "HeadObject", varargs...)
	ret0, _ := ret[0].(*s3.HeadObjectOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// HeadObject indicates an expected call of HeadObject.
func (mr *MockS3APIMockRecorder) HeadObject(ctx, input any, opt ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, input}, opt...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HeadObject", reflect.TypeOf((*MockS3API)(nil).HeadObject), varargs...)
}

// ListParts mocks base method.
func (m *MockS3API) ListParts(ctx context.Context, input *s3.ListPartsInput, opt ...func(*s3.Options)) (*s3.ListPartsOutput, error) {
	m.ctrl.T.Helper()
	varargs := []any{ctx, input}
	for _, a := range opt {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ListParts", varargs...)
	ret0, _ := ret[0].(*s3.ListPartsOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListParts indicates an expected call of ListParts.
func (mr *MockS3APIMockRecorder) ListParts(ctx, input any, opt ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, input}, opt...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListParts", reflect.TypeOf((*MockS3API)(nil).ListParts), varargs...)
}

// PutObject mocks base method.
func (m *MockS3API) PutObject(ctx context.Context, input *s3.PutObjectInput, opt ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.ctrl.T.Helper()
	varargs := []any{ctx, input}
	for _, a := range opt {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "PutObject", varargs...)
	ret0, _ := ret[0].(*s3.PutObjectOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PutObject indicates an expected call of PutObject.
func (mr *MockS3APIMockRecorder) PutObject(ctx, input any, opt ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, input}, opt...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PutObject", reflect.TypeOf((*MockS3API)(nil).PutObject), varargs...)
}

// UploadPart mocks base method.
func (m *MockS3API) UploadPart(ctx context.Context, input *s3.UploadPartInput, opt ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	m.ctrl.T.Helper()
	varargs := []any{ctx, input}
	for _, a := range opt {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "UploadPart", varargs...)
	ret0, _ := ret[0].(*s3.UploadPartOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UploadPart indicates an expected call of UploadPart.
func (mr *MockS3APIMockRecorder) UploadPart(ctx, input any, opt ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, input}, opt...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UploadPart", reflect.TypeOf((*MockS3API)(nil).UploadPart), varargs...)
}

// UploadPartCopy mocks base method.
func (m *MockS3API) UploadPartCopy(ctx context.Context, input *s3.UploadPartCopyInput, opt ...func(*s3.Options)) (*s3.UploadPartCopyOutput, error) {
	m.ctrl.T.Helper()
	varargs := []any{ctx, input}
	for _, a := range opt {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "UploadPartCopy", varargs...)
	ret0, _ := ret[0].(*s3.UploadPartCopyOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UploadPartCopy indicates an expected call of UploadPartCopy.
func (mr *MockS3APIMockRecorder) UploadPartCopy(ctx, input any, opt ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, input}, opt...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UploadPartCopy", reflect.TypeOf((*MockS3API)(nil).UploadPartCopy), varargs...)
}

// MockClient is a mock of Client interface.
type MockClient struct {
	ctrl     *gomock.Controller
	recorder *MockClientMockRecorder
	isgomock struct{}
}

// MockClientMockRecorder is the mock recorder for MockClient.
type MockClientMockRecorder struct {
	mock *MockClient
}

// NewMockClient creates a new mock instance.
func NewMockClient(ctrl *gomock.Controller) *MockClient {
	mock := &MockClient{ctrl: ctrl}
	mock.recorder = &MockClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClient) EXPECT() *MockClientMockRecorder {
	return m.recorder
}

// GetConnectionID mocks base method.
func (m *MockClient) GetConnectionID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetConnectionID")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetConnectionID indicates an expected call of GetConnectionID.
func (mr *MockClientMockRecorder) GetConnectionID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetConnectionID", reflect.TypeOf((*MockClient)(nil).GetConnectionID))
}

// GetConnectionPool mocks base method.
func (m *MockClient) GetConnectionPool(logger logr.Logger) protoc.ConnectionPool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetConnectionPool", logger)
	ret0, _ := ret[0].(protoc.ConnectionPool)
	return ret0
}

// GetConnectionPool indicates an expected call of GetConnectionPool.
func (mr *MockClientMockRecorder) GetConnectionPool(logger any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetConnectionPool", reflect.TypeOf((*MockClient)(nil).GetConnectionPool), logger)
}

// GetCredential mocks base method.
func (m *MockClient) GetCredential() any {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCredential")
	ret0, _ := ret[0].(any)
	return ret0
}

// GetCredential indicates an expected call of GetCredential.
func (mr *MockClientMockRecorder) GetCredential() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCredential", reflect.TypeOf((*MockClient)(nil).GetCredential))
}

// GetS3API mocks base method.
func (m *MockClient) GetS3API() protoc.S3API {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetS3API")
	ret0, _ := ret[0].(protoc.S3API)
	return ret0
}

// GetS3API indicates an expected call of GetS3API.
func (mr *MockClientMockRecorder) GetS3API() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetS3API", reflect.TypeOf((*MockClient)(nil).GetS3API))
}
