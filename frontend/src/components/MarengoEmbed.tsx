import React, { useEffect, useState } from 'react';
import { authFetch } from '../authFetch';
import Alert from '@cloudscape-design/components/alert';
import Badge from '@cloudscape-design/components/badge';
import Box from '@cloudscape-design/components/box';
import Button from '@cloudscape-design/components/button';
import Container from '@cloudscape-design/components/container';
import Header from '@cloudscape-design/components/header';
import SpaceBetween from '@cloudscape-design/components/space-between';
import StatusIndicator from '@cloudscape-design/components/status-indicator';
import Table from '@cloudscape-design/components/table';

interface Video {
  key: string;
  filename: string;
  s3Uri: string;
  bucket: string;
  uploadedAt: string;
  isShared?: boolean;
}

interface EmbeddingInfo {
  videoId: string;
  status: string;
  invocationArn: string;
  segmentsCount: number;
  createdAt: string;
  completedAt: string;
}

const MarengoEmbed: React.FC = () => {
  const [videos, setVideos] = useState<Video[]>([]);
  const [embeddings, setEmbeddings] = useState<Map<string, EmbeddingInfo>>(new Map());
  const [loadingKey, setLoadingKey] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);

  const API_BASE_URL = (process.env.REACT_APP_API_URL || '').replace(/\/+$/, '');

  useEffect(() => { loadData(); }, []);

  useEffect(() => {
    const processingItems = Array.from(embeddings.values()).filter((e) => e.status === 'processing');
    if (processingItems.length === 0) return;
    const interval = setInterval(async () => {
      let anyCompleted = false;
      for (const emb of processingItems) {
        try {
          const res = await authFetch(`${API_BASE_URL}/status?invocationArn=${encodeURIComponent(emb.invocationArn)}`);
          const result = await res.json();
          if (result.status === 'Completed') {
            setStatusMessage(`Completed: ${result.segments_processed || 0} segments for ${emb.videoId.split('/').pop()}`);
            anyCompleted = true;
          }
        } catch (e) { console.error('Polling error:', e); }
      }
      if (anyCompleted) loadData();
    }, 5000);
    return () => clearInterval(interval);
  }, [embeddings]);

  const loadData = async () => {
    setRefreshing(true);
    try {
      const [videosRes, embeddingsRes] = await Promise.all([
        authFetch(`${API_BASE_URL}/videos`),
        authFetch(`${API_BASE_URL}/embeddings`),
      ]);
      const videosData = await videosRes.json();
      const embeddingsData = await embeddingsRes.json();
      setVideos(videosData.videos || []);
      const embMap = new Map<string, EmbeddingInfo>();
      (embeddingsData.embeddings || []).forEach((e: EmbeddingInfo) => {
        embMap.set(e.videoId, e);
      });
      setEmbeddings(embMap);
    } catch (e) {
      console.error('Failed to load data:', e);
    } finally {
      setRefreshing(false);
    }
  };

  const embedVideo = async (video: Video) => {
    setLoadingKey(video.key);
    setError(null);
    setStatusMessage(null);
    try {
      const res = await authFetch(`${API_BASE_URL}/embed`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ s3Uri: video.s3Uri, videoId: video.key }),
      });
      const result = await res.json();
      if (!res.ok) throw new Error(result.error || 'Embed failed');
      setStatusMessage(`Embedding started for ${video.filename}`);
      setEmbeddings((prev) => {
        const next = new Map(prev);
        next.set(video.key, {
          videoId: video.key,
          status: 'processing',
          invocationArn: result.invocationArn,
          segmentsCount: 0,
          createdAt: new Date().toISOString(),
          completedAt: '',
        });
        return next;
      });
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoadingKey(null);
    }
  };

  const statusCell = (video: Video) => {
    const info = embeddings.get(video.key);
    if (video.isShared) {
      return <StatusIndicator type="success">Pre-indexed (SHARED)</StatusIndicator>;
    }
    if (!info) return <StatusIndicator type="stopped">Not embedded</StatusIndicator>;
    if (info.status === 'completed') return <StatusIndicator type="success">Embedded ({info.segmentsCount || 0} segments)</StatusIndicator>;
    if (info.status === 'processing') return <StatusIndicator type="in-progress">Processing</StatusIndicator>;
    return <StatusIndicator type="warning">{info.status}</StatusIndicator>;
  };

  const actionCell = (video: Video) => {
    if (video.isShared) return <Box variant="small" color="text-status-inactive">N/A</Box>;
    const info = embeddings.get(video.key);
    const status = info?.status || 'not_embedded';
    if (status === 'not_embedded') {
      return (
        <Button
          onClick={() => embedVideo(video)}
          loading={loadingKey === video.key}
          disabled={loadingKey !== null}
        >
          Embed
        </Button>
      );
    }
    if (status === 'processing') return <Badge color="blue">Polling...</Badge>;
    return <Box variant="small">-</Box>;
  };

  return (
    <SpaceBetween size="l">
      {statusMessage && <Alert type="success" dismissible onDismiss={() => setStatusMessage(null)}>{statusMessage}</Alert>}
      {error && <Alert type="error" dismissible onDismiss={() => setError(null)}>{error}</Alert>}

      <Container
        header={
          <Header
            variant="h2"
            description="본인이 업로드한 영상을 10초 단위 512차원 벡터로 임베딩하여 Marengo Search에서 검색 가능하게 합니다. SHARED 영상은 이미 사전 인덱싱돼 있으니 Marengo Search를 바로 사용하세요."
            counter={`(${videos.length})`}
            actions={<Button iconName="refresh" onClick={loadData} loading={refreshing} />}
          >
            Marengo Embed
          </Header>
        }
      >
        <Table
          items={videos}
          trackBy={(v) => `${v.isShared ? 'shared' : 'mine'}-${v.key}`}
          columnDefinitions={[
            {
              id: 'filename',
              header: 'Filename',
              cell: (v) => (
                <SpaceBetween direction="horizontal" size="xs">
                  {v.isShared && <Badge color="blue">SHARED</Badge>}
                  <span>{v.filename}</span>
                </SpaceBetween>
              ),
            },
            { id: 'uploadedAt', header: 'Uploaded', cell: (v) => v.uploadedAt || '-' },
            { id: 'status', header: 'Status', cell: statusCell },
            { id: 'action', header: 'Action', cell: actionCell },
          ]}
          empty={
            <Box textAlign="center" color="inherit" padding={{ vertical: 'l' }}>
              <b>No videos available</b>
              <Box variant="p" color="inherit">Upload a video in the Upload tab first.</Box>
            </Box>
          }
        />
      </Container>
    </SpaceBetween>
  );
};

export default MarengoEmbed;
