use super::{
    EtcdDeleteRequest, EtcdDeleteResponse, EtcdPutRequest, EtcdPutResponse, EtcdRangeRequest,
    EtcdRangeResponse, KeyRange,
};
use crate::protos::rpc::{
    Compare, Compare_CompareResult, Compare_CompareTarget, Compare_oneof_target_union, RequestOp,
    ResponseOp, ResponseOp_oneof_response, TxnRequest, TxnResponse,
};
use crate::ResponseHeader;
use protobuf::RepeatedField;
use utilities::Cast;

/// Request for performing transaction operations.
#[derive(Debug, Clone)]
pub struct EtcdTxnRequest {
    /// Etcd transaction operations request.
    proto: TxnRequest,
}

impl EtcdTxnRequest {
    /// Creates a new `TxnRequest`.
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        let txn_request = TxnRequest {
            compare: RepeatedField::from_vec(vec![]),
            success: RepeatedField::from_vec(vec![]),
            failure: RepeatedField::from_vec(vec![]),
            ..TxnRequest::default()
        };
        Self { proto: txn_request }
    }

    /// Adds a version compare.
    #[inline]
    pub fn when_version(mut self, key_range: KeyRange, cmp: TxnCmp, version: usize) -> Self {
        let compare_result: Compare_CompareResult = cmp.into();
        let compare = Compare {
            result: compare_result,
            target: Compare_CompareTarget::VERSION,
            key: key_range.key,
            range_end: key_range.range_end,
            target_union: Some(Compare_oneof_target_union::version(version.cast())),
            ..Compare::default()
        };
        self.proto.compare.push(compare);
        self
    }

    /// Adds a create revision compare.
    #[inline]
    pub fn when_create_revision(
        mut self,
        key_range: KeyRange,
        cmp: TxnCmp,
        revision: usize,
    ) -> Self {
        let compare_result: Compare_CompareResult = cmp.into();
        let compare = Compare {
            result: compare_result,
            target: Compare_CompareTarget::CREATE,
            key: key_range.key,
            range_end: key_range.range_end,
            target_union: Some(Compare_oneof_target_union::create_revision(revision.cast())),
            ..Compare::default()
        };
        self.proto.compare.push(compare);
        self
    }

    /// Adds a mod revision compare.
    #[inline]
    pub fn when_mod_revision(mut self, key_range: KeyRange, cmp: TxnCmp, revision: usize) -> Self {
        let compare_result: Compare_CompareResult = cmp.into();
        let compare = Compare {
            result: compare_result,
            target: Compare_CompareTarget::MOD,
            key: key_range.key,
            range_end: key_range.range_end,
            target_union: Some(Compare_oneof_target_union::mod_revision(revision.cast())),
            ..Compare::default()
        };
        self.proto.compare.push(compare);
        self
    }

    /// Adds a value compare.
    #[inline]
    pub fn when_value<V>(mut self, key_range: KeyRange, cmp: TxnCmp, value: V) -> Self
    where
        V: Into<Vec<u8>>,
    {
        let compare_result: Compare_CompareResult = cmp.into();
        let compare = Compare {
            result: compare_result,
            target: Compare_CompareTarget::VALUE,
            key: key_range.key,
            range_end: key_range.range_end,
            target_union: Some(Compare_oneof_target_union::value(value.into())),
            ..Compare::default()
        };
        self.proto.compare.push(compare);
        self
    }

    /// If compare success, then execute the specified operations.
    #[inline]
    pub fn and_then<O>(mut self, op: O) -> Self
    where
        O: Into<TxnOp>,
    {
        self.proto.success.push(op.into().into());
        self
    }

    /// If compare fail, then execute the specified operations.
    #[inline]
    pub fn or_else<O>(mut self, op: O) -> Self
    where
        O: Into<TxnOp>,
    {
        self.proto.failure.push(op.into().into());
        self
    }

    /// Get the success operations from `TxnRequest`.
    #[inline]
    pub fn get_success_operations(&self) -> Vec<RequestOp> {
        self.proto.success.to_vec()
    }

    /// Get the failure operations from `TxnRequest`.
    #[inline]
    pub fn get_failure_operations(&self) -> Vec<RequestOp> {
        self.proto.failure.to_vec()
    }
}

impl Default for EtcdTxnRequest {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Into<TxnRequest> for EtcdTxnRequest {
    #[inline]
    fn into(self) -> TxnRequest {
        self.proto
    }
}

/// Transaction Operation.
pub enum TxnOp {
    /// Range fetching operation.
    Range(EtcdRangeRequest),
    /// Put operation.
    Put(EtcdPutRequest),
    /// Delete operation.
    Delete(EtcdDeleteRequest),
    /// Txn operation.
    Txn(EtcdTxnRequest),
}

impl Into<RequestOp> for TxnOp {
    fn into(self) -> RequestOp {
        let mut request_op = RequestOp::new();
        match self {
            Self::Range(req) => request_op.set_request_range(req.into()),
            Self::Put(req) => request_op.set_request_put(req.into()),
            Self::Delete(req) => request_op.set_request_delete_range(req.into()),
            Self::Txn(req) => request_op.set_request_txn(req.into()),
        };
        request_op
    }
}

impl From<EtcdRangeRequest> for TxnOp {
    fn from(req: EtcdRangeRequest) -> Self {
        Self::Range(req)
    }
}

impl From<EtcdPutRequest> for TxnOp {
    fn from(req: EtcdPutRequest) -> Self {
        Self::Put(req)
    }
}

impl From<EtcdDeleteRequest> for TxnOp {
    fn from(req: EtcdDeleteRequest) -> Self {
        Self::Delete(req)
    }
}

impl From<EtcdTxnRequest> for TxnOp {
    fn from(req: EtcdTxnRequest) -> Self {
        Self::Txn(req)
    }
}

/// Transaction Comparation.
#[derive(Clone, Copy)]
pub enum TxnCmp {
    /// Equal comparation.
    Equal,
    /// NotEqual comparation.
    NotEqual,
    /// Greater comparation.
    Greater,
    /// Less comparation.
    Less,
}

impl Into<Compare_CompareResult> for TxnCmp {
    #[inline]
    fn into(self) -> Compare_CompareResult {
        match self {
            Self::Equal => Compare_CompareResult::EQUAL,
            Self::NotEqual => Compare_CompareResult::NOT_EQUAL,
            Self::Greater => Compare_CompareResult::GREATER,
            Self::Less => Compare_CompareResult::LESS,
        }
    }
}

/// Response transaction operation.
pub enum TxnOpResponse {
    /// Range reponse.
    Range(EtcdRangeResponse),
    /// Put reponse.
    Put(EtcdPutResponse),
    /// Delete response.
    Delete(EtcdDeleteResponse),
    /// Transaction response.
    Txn(EtcdTxnResponse),
}

impl From<ResponseOp> for TxnOpResponse {
    #[inline]
    fn from(mut resp: ResponseOp) -> Self {
        match resp
            .response
            .take()
            .unwrap_or_else(|| panic!("Fail to get TxnOpResponse"))
        {
            ResponseOp_oneof_response::response_range(r) => Self::Range(From::from(r)),
            ResponseOp_oneof_response::response_put(r) => Self::Put(From::from(r)),
            ResponseOp_oneof_response::response_txn(r) => Self::Txn(From::from(r)),
            ResponseOp_oneof_response::response_delete_range(r) => Self::Delete(From::from(r)),
        }
    }
}

/// Response for transaction.
#[derive(Debug, Clone)]
pub struct EtcdTxnResponse {
    /// Etcd transaction operations request.
    proto: TxnResponse,
}

impl EtcdTxnResponse {
    /// Takes the header out of response, leaving a `None` in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        match self.proto.header.take() {
            Some(header) => Some(From::from(header)),
            None => None,
        }
    }

    /// Returns `true` if the compare evaluated to true, and `false` otherwise.
    #[inline]
    pub const fn is_success(&self) -> bool {
        self.proto.succeeded
    }

    /// Takes the responses corresponding to the results from applying the
    /// Success block if succeeded is true or the Failure if succeeded is false.
    #[inline]
    pub fn take_responses(&mut self) -> Vec<TxnOpResponse> {
        let responses = std::mem::take(&mut self.proto.responses);

        responses.into_iter().map(From::from).collect()
    }

    /// Takes the responses corresponding to the results from applying the
    /// Success block if succeeded is true or the Failure if succeeded is false.
    #[inline]
    pub fn get_responses(&self) -> Vec<TxnOpResponse> {
        self.proto
            .responses
            .clone()
            .into_iter()
            .map(From::from)
            .collect()
    }
}

impl From<TxnResponse> for EtcdTxnResponse {
    #[inline]
    fn from(resp: TxnResponse) -> Self {
        Self { proto: resp }
    }
}
