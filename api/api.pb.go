// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.21.9
// source: api/api.proto

package api

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type SetRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key   string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *SetRequest) Reset() {
	*x = SetRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_api_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SetRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SetRequest) ProtoMessage() {}

func (x *SetRequest) ProtoReflect() protoreflect.Message {
	mi := &file_api_api_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SetRequest.ProtoReflect.Descriptor instead.
func (*SetRequest) Descriptor() ([]byte, []int) {
	return file_api_api_proto_rawDescGZIP(), []int{0}
}

func (x *SetRequest) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *SetRequest) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

type GetRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
}

func (x *GetRequest) Reset() {
	*x = GetRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_api_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetRequest) ProtoMessage() {}

func (x *GetRequest) ProtoReflect() protoreflect.Message {
	mi := &file_api_api_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetRequest.ProtoReflect.Descriptor instead.
func (*GetRequest) Descriptor() ([]byte, []int) {
	return file_api_api_proto_rawDescGZIP(), []int{1}
}

func (x *GetRequest) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

type GetResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Value []byte `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *GetResponse) Reset() {
	*x = GetResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_api_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetResponse) ProtoMessage() {}

func (x *GetResponse) ProtoReflect() protoreflect.Message {
	mi := &file_api_api_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetResponse.ProtoReflect.Descriptor instead.
func (*GetResponse) Descriptor() ([]byte, []int) {
	return file_api_api_proto_rawDescGZIP(), []int{2}
}

func (x *GetResponse) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

type Empty struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *Empty) Reset() {
	*x = Empty{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_api_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Empty) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Empty) ProtoMessage() {}

func (x *Empty) ProtoReflect() protoreflect.Message {
	mi := &file_api_api_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Empty.ProtoReflect.Descriptor instead.
func (*Empty) Descriptor() ([]byte, []int) {
	return file_api_api_proto_rawDescGZIP(), []int{3}
}

type Server struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id       string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	RpcAddr  string `protobuf:"bytes,2,opt,name=rpc_addr,json=rpcAddr,proto3" json:"rpc_addr,omitempty"`
	IsLeader bool   `protobuf:"varint,3,opt,name=is_leader,json=isLeader,proto3" json:"is_leader,omitempty"`
	// suffrage but this wording is simpler
	VoteStatus string `protobuf:"bytes,4,opt,name=vote_status,json=voteStatus,proto3" json:"vote_status,omitempty"`
}

func (x *Server) Reset() {
	*x = Server{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_api_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Server) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Server) ProtoMessage() {}

func (x *Server) ProtoReflect() protoreflect.Message {
	mi := &file_api_api_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Server.ProtoReflect.Descriptor instead.
func (*Server) Descriptor() ([]byte, []int) {
	return file_api_api_proto_rawDescGZIP(), []int{4}
}

func (x *Server) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Server) GetRpcAddr() string {
	if x != nil {
		return x.RpcAddr
	}
	return ""
}

func (x *Server) GetIsLeader() bool {
	if x != nil {
		return x.IsLeader
	}
	return false
}

func (x *Server) GetVoteStatus() string {
	if x != nil {
		return x.VoteStatus
	}
	return ""
}

type GetServer struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Server []*Server `protobuf:"bytes,1,rep,name=server,proto3" json:"server,omitempty"`
}

func (x *GetServer) Reset() {
	*x = GetServer{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_api_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetServer) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetServer) ProtoMessage() {}

func (x *GetServer) ProtoReflect() protoreflect.Message {
	mi := &file_api_api_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetServer.ProtoReflect.Descriptor instead.
func (*GetServer) Descriptor() ([]byte, []int) {
	return file_api_api_proto_rawDescGZIP(), []int{5}
}

func (x *GetServer) GetServer() []*Server {
	if x != nil {
		return x.Server
	}
	return nil
}

var File_api_api_proto protoreflect.FileDescriptor

var file_api_api_proto_rawDesc = []byte{
	0x0a, 0x0d, 0x61, 0x70, 0x69, 0x2f, 0x61, 0x70, 0x69, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x03, 0x61, 0x70, 0x69, 0x22, 0x34, 0x0a, 0x0a, 0x53, 0x65, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0c, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x1e, 0x0a, 0x0a, 0x47, 0x65,
	0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x22, 0x23, 0x0a, 0x0b, 0x47, 0x65,
	0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c,
	0x75, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22,
	0x07, 0x0a, 0x05, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x22, 0x71, 0x0a, 0x06, 0x53, 0x65, 0x72, 0x76,
	0x65, 0x72, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02,
	0x69, 0x64, 0x12, 0x19, 0x0a, 0x08, 0x72, 0x70, 0x63, 0x5f, 0x61, 0x64, 0x64, 0x72, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x72, 0x70, 0x63, 0x41, 0x64, 0x64, 0x72, 0x12, 0x1b, 0x0a,
	0x09, 0x69, 0x73, 0x5f, 0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x18, 0x03, 0x20, 0x01, 0x28, 0x08,
	0x52, 0x08, 0x69, 0x73, 0x4c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x12, 0x1f, 0x0a, 0x0b, 0x76, 0x6f,
	0x74, 0x65, 0x5f, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0a, 0x76, 0x6f, 0x74, 0x65, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x22, 0x30, 0x0a, 0x09, 0x47,
	0x65, 0x74, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x12, 0x23, 0x0a, 0x06, 0x73, 0x65, 0x72, 0x76,
	0x65, 0x72, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0b, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x53,
	0x65, 0x72, 0x76, 0x65, 0x72, 0x52, 0x06, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x32, 0x7f, 0x0a,
	0x05, 0x43, 0x61, 0x63, 0x68, 0x65, 0x12, 0x22, 0x0a, 0x03, 0x53, 0x65, 0x74, 0x12, 0x0f, 0x2e,
	0x61, 0x70, 0x69, 0x2e, 0x53, 0x65, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x0a,
	0x2e, 0x61, 0x70, 0x69, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x12, 0x28, 0x0a, 0x03, 0x47, 0x65,
	0x74, 0x12, 0x0f, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x47, 0x65, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x1a, 0x10, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x47, 0x65, 0x74, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x12, 0x28, 0x0a, 0x0a, 0x47, 0x65, 0x74, 0x53, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x73, 0x12, 0x0a, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x1a, 0x0e,
	0x2e, 0x61, 0x70, 0x69, 0x2e, 0x47, 0x65, 0x74, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x42, 0x1d,
	0x5a, 0x1b, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x6e, 0x69, 0x72,
	0x65, 0x6f, 0x2f, 0x64, 0x63, 0x61, 0x63, 0x68, 0x65, 0x2f, 0x61, 0x70, 0x69, 0x62, 0x06, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_api_api_proto_rawDescOnce sync.Once
	file_api_api_proto_rawDescData = file_api_api_proto_rawDesc
)

func file_api_api_proto_rawDescGZIP() []byte {
	file_api_api_proto_rawDescOnce.Do(func() {
		file_api_api_proto_rawDescData = protoimpl.X.CompressGZIP(file_api_api_proto_rawDescData)
	})
	return file_api_api_proto_rawDescData
}

var file_api_api_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_api_api_proto_goTypes = []interface{}{
	(*SetRequest)(nil),  // 0: api.SetRequest
	(*GetRequest)(nil),  // 1: api.GetRequest
	(*GetResponse)(nil), // 2: api.GetResponse
	(*Empty)(nil),       // 3: api.Empty
	(*Server)(nil),      // 4: api.Server
	(*GetServer)(nil),   // 5: api.GetServer
}
var file_api_api_proto_depIdxs = []int32{
	4, // 0: api.GetServer.server:type_name -> api.Server
	0, // 1: api.Cache.Set:input_type -> api.SetRequest
	1, // 2: api.Cache.Get:input_type -> api.GetRequest
	3, // 3: api.Cache.GetServers:input_type -> api.Empty
	3, // 4: api.Cache.Set:output_type -> api.Empty
	2, // 5: api.Cache.Get:output_type -> api.GetResponse
	5, // 6: api.Cache.GetServers:output_type -> api.GetServer
	4, // [4:7] is the sub-list for method output_type
	1, // [1:4] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_api_api_proto_init() }
func file_api_api_proto_init() {
	if File_api_api_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_api_api_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SetRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_api_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_api_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_api_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Empty); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_api_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Server); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_api_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetServer); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_api_api_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_api_api_proto_goTypes,
		DependencyIndexes: file_api_api_proto_depIdxs,
		MessageInfos:      file_api_api_proto_msgTypes,
	}.Build()
	File_api_api_proto = out.File
	file_api_api_proto_rawDesc = nil
	file_api_api_proto_goTypes = nil
	file_api_api_proto_depIdxs = nil
}
