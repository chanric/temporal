package historydlq

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	commonspb "go.temporal.io/server/api/common/v1"
)

func testEnqueueRequest() EnqueueRequest {
	req := testRequest()
	req.Task = &commonspb.HistoryTask{
		ShardId: 7,
		Blob: &commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         []byte("task payload"),
		},
	}
	return req
}
