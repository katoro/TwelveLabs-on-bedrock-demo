import React, { useEffect, useState } from 'react';
import { authFetch } from '../authFetch';
import VideoPlayer from './VideoPlayer';
import Alert from '@cloudscape-design/components/alert';
import Badge from '@cloudscape-design/components/badge';
import Box from '@cloudscape-design/components/box';
import Button from '@cloudscape-design/components/button';
import ColumnLayout from '@cloudscape-design/components/column-layout';
import Container from '@cloudscape-design/components/container';
import FormField from '@cloudscape-design/components/form-field';
import Header from '@cloudscape-design/components/header';
import Select, { SelectProps } from '@cloudscape-design/components/select';
import SpaceBetween from '@cloudscape-design/components/space-between';
import StatusIndicator from '@cloudscape-design/components/status-indicator';
import Textarea from '@cloudscape-design/components/textarea';

interface Video {
  key: string;
  filename: string;
  s3Uri: string;
  bucket: string;
  uploadedAt: string;
  isShared?: boolean;
}

interface Analysis {
  jobId: string;
  videoId: string;
  prompt: string;
  status: string;
  analysis: string;
  error: string;
  createdAt: string;
  completedAt: string;
}

const PegasusAnalyze: React.FC = () => {
  const [videos, setVideos] = useState<Video[]>([]);
  const [selectedOption, setSelectedOption] = useState<SelectProps.Option | null>(null);
  const [prompt, setPrompt] = useState('Analyze this video and provide a detailed description.');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [analysisJobId, setAnalysisJobId] = useState<string | null>(null);
  const [analysisStatus, setAnalysisStatus] = useState<string | null>(null);
  const [analyses, setAnalyses] = useState<Analysis[]>([]);

  const API_BASE_URL = (process.env.REACT_APP_API_URL || '').replace(/\/+$/, '');

  const selectedVideo = videos.find((v) => v.key === selectedOption?.value) || null;

  useEffect(() => { loadVideos(); }, []);

  useEffect(() => {
    if (selectedVideo) loadAnalyses(selectedVideo.key);
  }, [selectedVideo?.key]);

  useEffect(() => {
    if (!analysisJobId) return;
    const interval = setInterval(async () => {
      try {
        const res = await authFetch(`${API_BASE_URL}/status?analysisJobId=${encodeURIComponent(analysisJobId)}`);
        const result = await res.json();
        if (result.status === 'Completed') {
          setAnalysisStatus(`Completed (${result.processingTime || 0}s)`);
          setAnalysisJobId(null);
          if (selectedVideo) loadAnalyses(selectedVideo.key);
        } else if (result.status === 'Failed') {
          setError(result.error || 'Analysis failed');
          setAnalysisStatus(null);
          setAnalysisJobId(null);
        } else {
          setAnalysisStatus(`Processing... (${result.message || ''})`);
        }
      } catch (e) { console.error('Polling error:', e); }
    }, 5000);
    return () => clearInterval(interval);
  }, [analysisJobId, selectedVideo?.key]);

  const loadVideos = async () => {
    try {
      const res = await authFetch(`${API_BASE_URL}/videos`);
      const data = await res.json();
      setVideos(data.videos || []);
    } catch (e) { console.error('Failed to load videos:', e); }
  };

  const loadAnalyses = async (videoId: string) => {
    try {
      const res = await authFetch(`${API_BASE_URL}/analyses?videoId=${encodeURIComponent(videoId)}`);
      const data = await res.json();
      setAnalyses(data.analyses || []);
    } catch (e) { console.error('Failed to load analyses:', e); }
  };

  const analyzeVideo = async () => {
    if (!selectedVideo) return;
    setLoading(true);
    setError(null);
    setAnalysisJobId(null);
    setAnalysisStatus(null);
    try {
      const res = await authFetch(`${API_BASE_URL}/analyze`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ s3Uri: selectedVideo.s3Uri, videoId: selectedVideo.key, prompt }),
      });
      const result = await res.json();
      if (!res.ok) throw new Error(result.error || 'Analysis failed');
      setAnalysisJobId(result.analysisJobId);
      setAnalysisStatus('Analysis started. Polling for result...');
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const statusToIndicator = (s: string) => {
    if (s === 'Completed') return <StatusIndicator type="success">Completed</StatusIndicator>;
    if (s === 'Failed') return <StatusIndicator type="error">Failed</StatusIndicator>;
    return <StatusIndicator type="in-progress">{s}</StatusIndicator>;
  };

  const videoOptions: SelectProps.Options = videos.map((v) => ({
    label: v.filename,
    value: v.key,
    description: v.uploadedAt,
    tags: v.isShared ? ['SHARED'] : undefined,
  }));

  return (
    <ColumnLayout columns={2} minColumnWidth={420}>
      <Container
        header={
          <Header
            variant="h2"
            description="Pegasus 1.2로 영상을 분석합니다. 공용(SHARED) 영상 또는 본인이 업로드한 영상을 선택하세요."
          >
            Analyze
          </Header>
        }
      >
        <SpaceBetween size="m">
          <FormField label="Video">
            <Select
              selectedOption={selectedOption}
              onChange={({ detail }) => setSelectedOption(detail.selectedOption)}
              options={videoOptions}
              placeholder="Select a video"
              empty="No videos available"
              filteringType="auto"
            />
          </FormField>

          <FormField label="Prompt" description="무엇을 분석하거나 요약할지 자연어로 지시해주세요.">
            <SpaceBetween size="xs">
              <SpaceBetween direction="horizontal" size="xs">
                <Button onClick={() => {
                  setPrompt('당신은 영상 컨텐츠를 제작하는 업무를 맡고 있는 마케팅 담당 부서 직원입니다.\n제공된 영상을 기반으로, 핵심 내용만 추려서 짧은 영상을 만드는 업무를 수행해야 합니다.\n해당 영상의 내용을 요약해주시고, 영상에서 주요 장면들을 timestamp와 함께 답변해주세요.');
                  const v = videos.find((v) => v.filename.includes('EP04_clip1_00-05'));
                  if (v) setSelectedOption({ label: v.filename, value: v.key, description: v.uploadedAt });
                }}>영상 요약</Button>
                <Button onClick={() => {
                  setPrompt('저는 큰 주택을 가지고 있는 에어비앤비 호스트입니다.\n숙소를 홍보하는 영상을 만들고 싶은데,\n영상에서 전반적인 숙소가 보이는 영상 부분을 찾아주시고 timestamp을 제공해주세요.');
                  const v = videos.find((v) => v.filename.includes('EP04_clip2_05-10'));
                  if (v) setSelectedOption({ label: v.filename, value: v.key, description: v.uploadedAt });
                }}>숙소 장면</Button>
              </SpaceBetween>
              <Textarea
                value={prompt}
                onChange={({ detail }) => setPrompt(detail.value)}
                rows={4}
                placeholder="Enter your analysis prompt"
              />
            </SpaceBetween>
          </FormField>

          <SpaceBetween direction="horizontal" size="xs">
            <Button
              variant="primary"
              onClick={analyzeVideo}
              loading={loading || analysisJobId !== null}
              disabled={!selectedVideo || !prompt.trim()}
            >
              Analyze
            </Button>
            {analysisStatus && <Box padding={{ top: 'xs' }}>{analysisStatus}</Box>}
          </SpaceBetween>

          {error && <Alert type="error" dismissible onDismiss={() => setError(null)}>{error}</Alert>}

          {selectedVideo && (
            <FormField label="Video Preview">
              <VideoPlayer
                videoS3Uri={selectedVideo.s3Uri}
                autoPlay={false}
                onError={(e) => console.error('Video preview error:', e)}
              />
            </FormField>
          )}
        </SpaceBetween>
      </Container>

      <Container
        header={
          <Header
            variant="h2"
            counter={selectedVideo ? `(${analyses.length})` : undefined}
            description="선택한 영상에 대해 Pegasus를 호출했던 이력입니다."
          >
            Analysis History
          </Header>
        }
      >
        {!selectedVideo ? (
          <Box textAlign="center" color="text-status-inactive" padding={{ vertical: 'l' }}>
            Select a video to see history.
          </Box>
        ) : analyses.length === 0 ? (
          <Box textAlign="center" color="text-status-inactive" padding={{ vertical: 'l' }}>
            No analyses yet for this video.
          </Box>
        ) : (
          <SpaceBetween size="m">
            {analyses.map((a, i) => (
              <Container
                key={a.jobId || i}
                header={
                  <Header
                    variant="h3"
                    actions={<Badge>{a.createdAt || '-'}</Badge>}
                  >
                    {statusToIndicator(a.status)}
                  </Header>
                }
              >
                <SpaceBetween size="s">
                  <Box variant="small" color="text-body-secondary">
                    {a.prompt}
                  </Box>
                  {a.analysis && <Box>{a.analysis}</Box>}
                  {a.error && <Alert type="error">{a.error}</Alert>}
                </SpaceBetween>
              </Container>
            ))}
          </SpaceBetween>
        )}
      </Container>
    </ColumnLayout>
  );
};

export default PegasusAnalyze;
