use {
    solana_hook_service_proto::hook_service::{
        hook_service_server::{HookService, HookServiceServer},
        EnableTpuRequest, EnableTpuResponse, MarkAccountsForDiscardRequest,
        MarkAccountsForDiscardResponse, SendBundleRequest, SendBundleResponse, SendPacketsRequest,
        SendPacketsResponse, SetMaxComputeRequest, SetMaxComputeResponse,
        SetShredForwarderAddressRequest, SetShredForwarderAddressResponse, SetTpuAddressRequest,
        SetTpuAddressResponse,
    },
    std::path::{Path, PathBuf},
    thiserror::Error,
    tokio::{fs::create_dir_all, net::UnixListener, task::JoinHandle},
    tokio_stream::wrappers::{ReceiverStream, UnixListenerStream},
    tonic::{async_trait, transport::Server, Request, Response, Status, Streaming},
};

#[derive(Debug, Error)]
pub enum HookServiceError {
    #[error("UdsPathError")]
    UdsPathError,

    #[error("UdsSetupError: {0}")]
    UdsSetupError(#[from] std::io::Error),

    #[error("TonicError: {0}")]
    TonicError(#[from] tonic::transport::Error),
}

pub type HookServiceResult<T> = Result<T, HookServiceError>;

pub struct HookGrpcService {
    pub handle: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl HookGrpcService {
    pub async fn new(uds_path: PathBuf) -> HookServiceResult<Self> {
        create_dir_all(
            Path::new(&uds_path)
                .parent()
                .ok_or(HookServiceError::UdsPathError)?,
        )
        .await?;

        let service = HookServiceServer::new(HookServiceImpl::new());

        let uds = UnixListener::bind(uds_path)?;
        let uds_stream = UnixListenerStream::new(uds);

        let handle = tokio::spawn(
            Server::builder()
                .add_service(service)
                .serve_with_incoming(uds_stream),
        );

        Ok(HookGrpcService { handle })
    }
}

struct HookServiceImpl {}

impl HookServiceImpl {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl HookService for HookServiceImpl {
    async fn enable_tpu(
        &self,
        _request: Request<EnableTpuRequest>,
    ) -> Result<Response<EnableTpuResponse>, Status> {
        todo!()
    }

    async fn set_tpu_address(
        &self,
        _request: Request<SetTpuAddressRequest>,
    ) -> Result<Response<SetTpuAddressResponse>, Status> {
        todo!()
    }

    async fn send_packets(
        &self,
        _request: Request<Streaming<SendPacketsRequest>>,
    ) -> Result<Response<SendPacketsResponse>, Status> {
        todo!()
    }

    async fn mark_accounts_for_discard(
        &self,
        _request: Request<MarkAccountsForDiscardRequest>,
    ) -> Result<Response<MarkAccountsForDiscardResponse>, Status> {
        todo!()
    }

    async fn set_shred_forwarder_address(
        &self,
        _request: Request<SetShredForwarderAddressRequest>,
    ) -> Result<Response<SetShredForwarderAddressResponse>, Status> {
        todo!()
    }

    async fn set_max_compute(
        &self,
        _request: Request<SetMaxComputeRequest>,
    ) -> Result<Response<SetMaxComputeResponse>, Status> {
        todo!()
    }

    type SendBundleStream = ReceiverStream<Result<SendBundleResponse, Status>>;

    async fn send_bundle(
        &self,
        _request: Request<Streaming<SendBundleRequest>>,
    ) -> Result<Response<Self::SendBundleStream>, Status> {
        todo!()
    }
}
