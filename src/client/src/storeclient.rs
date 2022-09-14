// Copyright 2022 The template Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use runtime::{
    store_client::StoreClient,
    stream::error::{Error, Result},
    MutateRequest, MutateRequestUnion, MutateResponse, ReadRequest, ReadResponse, SealRequest,
    SealResponse, WriteRequest, WriteResponse,
};
use tonic::{transport::Channel, Streaming};

pub struct StorageClient {
    client: StoreClient<Channel>,
}

impl StorageClient {
    pub fn new(channel: Channel) -> Self {
        StorageClient {
            client: StoreClient::new(channel),
        }
    }

    pub async fn mutate(&self, input: MutateRequest) -> Result<MutateResponse> {
        let mut client = self.client.clone();
        let resp = client.mutate(input).await?;
        Ok(resp.into_inner())
    }

    pub async fn write(
        &self,
        stream_id: u64,
        writer_epoch: u32,
        input: WriteRequest,
    ) -> Result<WriteResponse> {
        let req = MutateRequest {
            stream_id,
            writer_epoch,
            request: Some(MutateRequestUnion {
                request: Some(runtime::mutate_request_union::Request::Write(input)),
            }),
        };

        let resp = self.mutate(req).await?;
        if let Some(runtime::mutate_response_union::Response::Write(res)) =
            resp.response.and_then(|r| r.response)
        {
            Ok(res)
        } else {
            Err(Error::InvalidResponse)
        }
    }

    pub async fn seal(
        &self,
        stream_id: u64,
        writer_epoch: u32,
        input: SealRequest,
    ) -> Result<SealResponse> {
        type Request = runtime::mutate_request_union::Request;
        type Response = runtime::mutate_response_union::Response;
        let req = MutateRequest {
            stream_id,
            writer_epoch,
            request: Some(MutateRequestUnion {
                request: Some(Request::Seal(input)),
            }),
        };

        let resp = self.mutate(req).await?;
        if let Some(Response::Seal(resp)) = resp.response.and_then(|r| r.response) {
            Ok(resp)
        } else {
            Err(Error::InvalidResponse)
        }
    }

    pub async fn read(&self, input: ReadRequest) -> Result<Streaming<ReadResponse>> {
        let mut client = self.client.clone();
        let resp = client.read(input).await?;
        Ok(resp.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use runtime::{
        stream::{
            error::Error,
            types::{Entry as sEntry, Sequence},
        },
        Entry, EntryType, ReadRequest, SealRequest, WriteRequest,
    };
    use tonic::transport::Endpoint;

    use super::Result;
    use crate::storeclient::StorageClient;

    fn entry(event: Vec<u8>) -> Entry {
        Entry {
            entry_type: EntryType::Event as i32,
            epoch: 1,
            event,
        }
    }

    async fn build_store_client() -> Result<StorageClient> {
        let addr = runtime::storage::build_store().await?;
        let endpoint = Endpoint::new(addr)?.connect().await?;
        Ok(StorageClient::new(endpoint))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn basic_write_and_read_acked() -> Result<()> {
        let writes = vec![
            WriteRequest {
                segment_epoch: 1,
                acked_seq: 0,
                first_index: 1,
                entries: vec![entry(vec![0u8]), entry(vec![2u8]), entry(vec![4u8])],
            },
            WriteRequest {
                segment_epoch: 1,
                acked_seq: Sequence::new(1, 3).into(),
                first_index: 4,
                entries: vec![entry(vec![6u8]), entry(vec![8u8])],
            },
            WriteRequest {
                acked_seq: Sequence::new(1, 5).into(),
                segment_epoch: 1,
                first_index: 6,
                entries: vec![],
            },
        ];

        let entries = vec![
            sEntry::Event {
                epoch: 1,
                event: vec![0u8].into(),
            },
            sEntry::Event {
                epoch: 1,
                event: vec![2u8].into(),
            },
            sEntry::Event {
                epoch: 1,
                event: vec![4u8].into(),
            },
            sEntry::Event {
                epoch: 1,
                event: vec![6u8].into(),
            },
            sEntry::Event {
                epoch: 1,
                event: vec![8u8].into(),
            },
        ];

        #[derive(Debug)]
        struct Test<'a> {
            from: u32,
            limit: u32,
            expect: &'a [sEntry],
        }

        let tests = vec![
            Test {
                from: 1,
                limit: 1,
                expect: &entries[0..1],
            },
            Test {
                from: 4,
                limit: 2,
                expect: &entries[3..],
            },
            Test {
                from: 1,
                limit: 5,
                expect: &entries[..],
            },
        ];

        let stream_id: u64 = 1;
        let writer_epoch: u32 = 1;
        let client = build_store_client().await?;

        for w in writes {
            client.write(stream_id, writer_epoch, w).await?;
        }
        // for test in tests {
        //     let req = ReadRequest {
        //         stream_id: 1,
        //         seg_epoch: 1,
        //         start_index: test.from,
        //         limit: test.limit,
        //         require_acked: true,
        //     };
        //     let mut stream = client.read(req).await?;
        //     let mut got = Vec::<sEntry>::new();
        //     while let Some(resp) = stream.next().await {
        //         got.push(resp?.entry.unwrap().into());
        //     }
        //     //println!("{:?}", got.len());
        //     // assert_eq!(got.len(), test.expect.len());
        //     // assert!(got.iter().zip(test.expect.iter()).all(|(l, r)| l == r));
        // }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn basic_write_and_read_including_pending_entries() -> Result<()> {
        let writes = vec![
            WriteRequest {
                segment_epoch: 1,
                acked_seq: 0,
                first_index: 1,
                entries: vec![entry(vec![0u8]), entry(vec![2u8]), entry(vec![4u8])],
            },
            WriteRequest {
                acked_seq: 0,
                segment_epoch: 1,
                first_index: 4,
                entries: vec![entry(vec![6u8]), entry(vec![8u8])],
            },
            WriteRequest {
                acked_seq: 0,
                segment_epoch: 1,
                first_index: 6,
                entries: vec![],
            },
        ];

        let stream_id: u64 = 1;
        let writer_epoch: u32 = 1;
        let entries = vec![
            sEntry::Event {
                epoch: 1,
                event: vec![0u8].into(),
            },
            sEntry::Event {
                epoch: 1,
                event: vec![2u8].into(),
            },
            sEntry::Event {
                epoch: 1,
                event: vec![4u8].into(),
            },
            sEntry::Event {
                epoch: 1,
                event: vec![6u8].into(),
            },
            sEntry::Event {
                epoch: 1,
                event: vec![8u8].into(),
            },
        ];

        struct Test<'a> {
            from: u32,
            limit: u32,
            expect: &'a [sEntry],
        }

        let tests = vec![
            Test {
                from: 1,
                limit: 1,
                expect: &entries[0..1],
            },
            Test {
                from: 4,
                limit: 2,
                expect: &entries[3..],
            },
            Test {
                from: 1,
                limit: 5,
                expect: &entries[..],
            },
            // require_acked is false, don't wait any entries
            Test {
                from: 1,
                limit: u32::MAX,
                expect: &entries[..],
            },
        ];

        let client = build_store_client().await?;
        for w in writes {
            client.write(stream_id, writer_epoch, w).await?;
        }

        for test in tests {
            let req = ReadRequest {
                stream_id: 1,
                seg_epoch: 1,
                start_index: test.from,
                limit: test.limit,
                require_acked: false,
            };
            let mut stream = client.read(req).await?;
            let mut got = Vec::<sEntry>::new();
            while let Some(resp) = stream.next().await {
                got.push(resp?.entry.unwrap().into());
            }
            assert_eq!(got.len(), test.expect.len());
            assert!(got.iter().zip(test.expect.iter()).all(|(l, r)| l == r));
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reject_staled_sealing_request() -> Result<()> {
        let client = build_store_client().await?;
        client.seal(1, 3, SealRequest { segment_epoch: 1 }).await?;

        match client.seal(1, 2, SealRequest { segment_epoch: 1 }).await {
            Err(Error::Staled(_)) => {}
            _ => {
                panic!("should reject staled sealing request");
            }
        };

        client.seal(1, 4, SealRequest { segment_epoch: 1 }).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reject_staled_writing_if_sealed() -> Result<()> {
        let client = build_store_client().await?;
        let write_req = WriteRequest {
            segment_epoch: 1,
            acked_seq: 0,
            first_index: 0,
            entries: vec![entry(vec![0u8]), entry(vec![2u8]), entry(vec![4u8])],
        };
        client.write(1, 1, write_req).await?;

        client.seal(1, 3, SealRequest { segment_epoch: 1 }).await?;

        let write_req = WriteRequest {
            segment_epoch: 1,
            acked_seq: Sequence::new(1, 2).into(),
            first_index: 3,
            entries: vec![entry(vec![6u8]), entry(vec![8u8])],
        };
        match client.write(1, 1, write_req).await {
            Err(Error::Staled(_)) => {}
            _ => {
                panic!("should reject staled store request");
            }
        };

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn remove_entries_once_receiving_bridge_entry() -> Result<()> {
        let client = build_store_client().await?;
        let write_req = WriteRequest {
            segment_epoch: 1,
            acked_seq: 0,
            first_index: 1,
            entries: vec![entry(vec![1u8]), entry(vec![2u8]), entry(vec![3u8])],
        };
        client.write(1, 1, write_req).await?;

        let write_req = WriteRequest {
            segment_epoch: 1,
            acked_seq: 0,
            first_index: 5,
            entries: vec![entry(vec![5u8])],
        };
        client.write(1, 1, write_req).await?;

        client.seal(1, 1, SealRequest { segment_epoch: 1 }).await?;

        let read_expect: Vec<Entry> = vec![
            entry(vec![1u8]).into(),
            entry(vec![2u8]).into(),
            entry(vec![3u8]).into(),
            entry(vec![5u8]).into(),
        ];
        let req = ReadRequest {
            stream_id: 1,
            seg_epoch: 1,
            start_index: 1,
            limit: u32::MAX,
            require_acked: false,
        };
        let mut stream = client.read(req).await?;
        let mut got = Vec::<Entry>::new();
        while let Some(resp) = stream.next().await {
            got.push(resp?.entry.unwrap().into());
        }
        assert_eq!(got.len(), read_expect.len());
        assert!(got.iter().zip(read_expect.iter()).all(|(l, r)| l == r));

        // send bridge record
        let bridge = Entry {
            entry_type: EntryType::Bridge as i32,
            epoch: 3,
            event: Vec::default(),
        };
        let write_req = WriteRequest {
            segment_epoch: 1,
            acked_seq: 0,
            first_index: 4,
            entries: vec![bridge.clone()],
        };
        client.write(1, 1, write_req).await?;

        let read_expect: Vec<Entry> = vec![
            entry(vec![1u8]).into(),
            entry(vec![2u8]).into(),
            entry(vec![3u8]).into(),
            bridge.into(),
        ];
        let req = ReadRequest {
            stream_id: 1,
            seg_epoch: 1,
            start_index: 1,
            limit: u32::MAX,
            require_acked: false,
        };
        let mut stream = client.read(req).await?;
        let mut got = Vec::<Entry>::new();
        while let Some(resp) = stream.next().await {
            got.push(resp?.entry.unwrap().into());
        }
        assert_eq!(got.len(), read_expect.len());
        assert!(got.iter().zip(read_expect.iter()).all(|(l, r)| l == r));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_returns_continuously_persisted_index() -> Result<()> {
        let client = build_store_client().await?;
        let write_req = WriteRequest {
            segment_epoch: 1,
            acked_seq: 0,
            first_index: 1,
            entries: vec![entry(vec![1u8]), entry(vec![2u8]), entry(vec![3u8])],
        };
        let resp = client.write(1, 1, write_req).await?;
        assert_eq!(resp.matched_index, 3);

        let write_req = WriteRequest {
            segment_epoch: 1,
            acked_seq: 0,
            first_index: 5,
            entries: vec![entry(vec![5u8])],
        };
        let resp = client.write(1, 1, write_req).await?;
        assert_eq!(resp.matched_index, 3);

        let write_req = WriteRequest {
            segment_epoch: 1,
            acked_seq: 0,
            first_index: 4,
            entries: vec![entry(vec![4u8])],
        };
        let resp = client.write(1, 1, write_req).await?;
        assert_eq!(resp.matched_index, 5);

        Ok(())
    }
}
