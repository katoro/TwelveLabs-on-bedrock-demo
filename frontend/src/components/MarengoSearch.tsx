import React, { useState } from 'react';
import { authFetch } from '../authFetch';
import VideoPlayer from './VideoPlayer';
import Alert from '@cloudscape-design/components/alert';
import Badge from '@cloudscape-design/components/badge';
import Box from '@cloudscape-design/components/box';
import Button from '@cloudscape-design/components/button';
import Cards from '@cloudscape-design/components/cards';
import Container from '@cloudscape-design/components/container';
import ExpandableSection from '@cloudscape-design/components/expandable-section';
import FormField from '@cloudscape-design/components/form-field';
import Header from '@cloudscape-design/components/header';
import Textarea from '@cloudscape-design/components/textarea';
import SpaceBetween from '@cloudscape-design/components/space-between';

interface SearchResult {
  videoId: string;
  videoS3Uri: string;
  segmentId: string;
  startSec: number;
  endSec: number;
  duration: number;
  embeddingOption: string;
  score: number;
  isShared?: boolean;
  metadata: { [key: string]: any };
}

interface SearchResponse {
  results: SearchResult[];
  total: number;
  search_time_ms: number;
  query: string;
  message: string;
}

const MarengoSearch: React.FC = () => {
  const [query, setQuery] = useState('');
  const [searchResponse, setSearchResponse] = useState<SearchResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedVideos, setExpandedVideos] = useState<Set<string>>(new Set());

  const API_BASE_URL = (process.env.REACT_APP_API_URL || '').replace(/\/+$/, '');

  const searchVideos = async () => {
    if (!query.trim()) return;
    setLoading(true);
    setError(null);
    setSearchResponse(null);
    try {
      const res = await authFetch(`${API_BASE_URL}/search?q=${encodeURIComponent(query)}`);
      if (!res.ok) throw new Error('Search failed');
      const data: SearchResponse = await res.json();
      setSearchResponse(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Search failed');
    } finally {
      setLoading(false);
    }
  };

  const formatTime = (seconds: number): string => {
    const m = Math.floor(seconds / 60);
    const s = Math.floor(seconds % 60);
    return `${m}:${s.toString().padStart(2, '0')}`;
  };

  const modalityLabel = (opt: string): string => {
    if (opt === 'audio') return 'Audio';
    if (opt === 'visual' || opt === 'visual-text') return 'Visual';
    return 'Mixed';
  };

  const keyOf = (r: SearchResult) => `${r.videoId}-${r.segmentId}-${r.startSec}`;

  return (
    <SpaceBetween size="l">
      <Container header={<Header variant="h2" description="자연어로 질의하면 Marengo 3.0이 512차원 벡터로 변환해 OpenSearch에서 유사도 검색을 수행합니다.">Query</Header>}>
        <SpaceBetween size="s">
          <SpaceBetween direction="horizontal" size="xs">
            <Button onClick={() => setQuery('당신은 영상 컨텐츠를 제작하는 업무를 맡고 있는 마케팅 담당 부서 직원입니다.\n제공된 영상을 기반으로, 가전제품이 나오는 부분만 이용해서 가전제품 광고 영상을 만들려고 합니다.\n가전제품이 화면에 나오는 장면 필요합니다.')}>가전제품 장면</Button>
            <Button onClick={() => setQuery('당신은 영상 컨텐츠를 제작하는 업무를 맡고 있는 마케팅 담당 부서 직원입니다.\n제공된 영상을 기반으로, TV가 나오는 부분만 이용해서 TV 광고 영상을 만들려고 합니다.\nTV가 화면에 나오는 장면 필요합니다.')}>TV 장면</Button>
            <Button onClick={() => setQuery('당신은 영상 컨텐츠를 제작하는 업무를 맡고 있는 마케팅 담당 부서 직원입니다.\n제공된 영상을 기반으로, 주방이 나오는 영상을 이용해서 광고 영상을 만드려고 합니다.\n주방이 나오는 장면이 필요합니다.')}>주방 장면</Button>
          </SpaceBetween>
          <FormField stretch>
            <div style={{ minHeight: 80 }}>
              <Textarea
                value={query}
                onChange={({ detail }) => setQuery(detail.value)}
                onKeyDown={({ detail }) => { if (detail.key === 'Enter' && !detail.shiftKey) searchVideos(); }}
                placeholder="예: 사람이 뛰어가는 장면, 격투 장면, 음식 먹는 장면"
                disabled={loading}
                rows={4}
              />
            </div>
          </FormField>
          <Button variant="primary" onClick={searchVideos} loading={loading} disabled={!query.trim()}>
            Search
          </Button>
        </SpaceBetween>
      </Container>

      {error && <Alert type="error" header="Search failed">{error}</Alert>}

      {searchResponse && (
        <Container
          header={
            <Header
              variant="h2"
              counter={`(${searchResponse.results.length}${searchResponse.total ? ` / ${searchResponse.total}` : ''})`}
              description={`${searchResponse.search_time_ms}ms · 점수는 cosine 유사도 기반 상대값이며 0.5~0.6이 일반적인 매칭 범위입니다.`}
            >
              Results
            </Header>
          }
        >
          {searchResponse.results.length === 0 ? (
            <Box textAlign="center" color="inherit" padding={{ vertical: 'xl' }}>
              <b>No results</b>
              <Box variant="p" color="inherit">{searchResponse.message || 'Try a different query.'}</Box>
            </Box>
          ) : (
            <Cards
              items={searchResponse.results}
              cardDefinition={{
                header: (r) => (
                  <SpaceBetween direction="horizontal" size="xs">
                    {r.isShared && <Badge color="blue">SHARED</Badge>}
                    <span>{r.videoId}</span>
                  </SpaceBetween>
                ),
                sections: [
                  {
                    id: 'meta',
                    content: (r) => (
                      <SpaceBetween direction="horizontal" size="s">
                        <Badge>{modalityLabel(r.embeddingOption)}</Badge>
                        <Box variant="small">
                          {formatTime(r.startSec)} – {formatTime(r.endSec)} ({Math.round(r.duration)}s)
                        </Box>
                        <Box variant="small" color="text-status-info">
                          score {r.score.toFixed(3)}
                        </Box>
                      </SpaceBetween>
                    ),
                  },
                  {
                    id: 'player',
                    content: (r) => {
                      const k = keyOf(r);
                      const expanded = expandedVideos.has(k);
                      const toggle = () => {
                        const next = new Set(expandedVideos);
                        if (expanded) next.delete(k); else next.add(k);
                        setExpandedVideos(next);
                      };
                      return (
                        <SpaceBetween size="s">
                          <Button onClick={toggle}>{expanded ? 'Hide' : 'Play'}</Button>
                          {expanded && (
                            <VideoPlayer
                              videoS3Uri={r.videoS3Uri}
                              startTime={r.startSec}
                              autoPlay={false}
                              className="search-result-video"
                              onError={(e) => console.error('Video error:', e)}
                            />
                          )}
                        </SpaceBetween>
                      );
                    },
                  },
                  {
                    id: 'details',
                    content: (r) => (
                      <ExpandableSection headerText="Details" variant="footer">
                        <Box variant="code">
                          <pre style={{ margin: 0, whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
{JSON.stringify({
  segmentId: r.segmentId,
  embeddingOption: r.embeddingOption,
  score: r.score,
  timeRange: `${r.startSec}s - ${r.endSec}s`,
  ...r.metadata,
}, null, 2)}
                          </pre>
                        </Box>
                      </ExpandableSection>
                    ),
                  },
                ],
              }}
              trackBy={keyOf}
              cardsPerRow={[{ cards: 1 }, { minWidth: 700, cards: 2 }]}
              empty={
                <Box textAlign="center" color="inherit">
                  <b>No results</b>
                </Box>
              }
            />
          )}
        </Container>
      )}

      {!searchResponse && !loading && !error && (
        <Box textAlign="center" color="text-status-inactive" padding={{ vertical: 'xl' }}>
          Enter a query above to find matching video segments.
        </Box>
      )}
    </SpaceBetween>
  );
};

export default MarengoSearch;
