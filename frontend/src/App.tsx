import React, { useState } from 'react';
import './App.css';
import { Authenticator } from '@aws-amplify/ui-react';
import '@aws-amplify/ui-react/styles.css';
import AppLayout from '@cloudscape-design/components/app-layout';
import TopNavigation from '@cloudscape-design/components/top-navigation';
import SideNavigation from '@cloudscape-design/components/side-navigation';
import ContentLayout from '@cloudscape-design/components/content-layout';
import Header from '@cloudscape-design/components/header';
import Box from '@cloudscape-design/components/box';
import SpaceBetween from '@cloudscape-design/components/space-between';
import Container from '@cloudscape-design/components/container';
import ColumnLayout from '@cloudscape-design/components/column-layout';
import Badge from '@cloudscape-design/components/badge';
import VideoUpload from './components/VideoUpload';
import PegasusAnalyze from './components/PegasusAnalyze';
import MarengoEmbed from './components/MarengoEmbed';
import MarengoSearch from './components/MarengoSearch';

type TabType = 'home' | 'upload' | 'analyze' | 'embed' | 'search';

const TAB_TITLES: Record<TabType, string> = {
  home: 'Overview',
  upload: 'Upload',
  analyze: 'Pegasus Analyze',
  embed: 'Marengo Embed',
  search: 'Marengo Search',
};

function App() {
  const [activeTab, setActiveTab] = useState<TabType>('home');

  return (
    <Authenticator>
      {({ signOut, user }) => (
        <>
          <div id="top-nav">
            <TopNavigation
              identity={{
                href: '#',
                title: 'Video Understanding',
                onFollow: (e) => { e.preventDefault(); setActiveTab('home'); },
              }}
              utilities={[
                {
                  type: 'button',
                  text: user?.signInDetails?.loginId ?? '',
                  iconName: 'user-profile',
                  disableUtilityCollapse: true,
                },
                {
                  type: 'button',
                  text: 'Sign out',
                  onClick: () => signOut?.(),
                },
              ]}
            />
          </div>
          <AppLayout
            headerSelector="#top-nav"
            toolsHide
            navigation={
              <SideNavigation
                activeHref={`#${activeTab}`}
                header={{ href: '#home', text: 'Video Understanding' }}
                onFollow={(e) => {
                  if (!e.detail.external) {
                    e.preventDefault();
                    const target = e.detail.href.replace('#', '') as TabType;
                    setActiveTab(target);
                  }
                }}
                items={[
                  { type: 'link', text: 'Overview', href: '#home' },
                  { type: 'link', text: 'Upload', href: '#upload' },
                  { type: 'link', text: 'Pegasus Analyze', href: '#analyze' },
                  { type: 'link', text: 'Marengo Embed', href: '#embed' },
                  { type: 'link', text: 'Marengo Search', href: '#search' },
                ]}
              />
            }
            content={
              <ContentLayout
                header={
                  <Header
                    variant="h1"
                    description={
                      activeTab === 'home'
                        ? 'Amazon Bedrock 기반 TwelveLabs AI 모델을 활용한 영상 분석 및 시맨틱 검색 PoC'
                        : undefined
                    }
                  >
                    {TAB_TITLES[activeTab]}
                  </Header>
                }
              >
                {activeTab === 'home' && <HomePlaceholder />}
                {activeTab === 'upload' && <VideoUpload />}
                {activeTab === 'analyze' && <PegasusAnalyze />}
                {activeTab === 'embed' && <MarengoEmbed />}
                {activeTab === 'search' && <MarengoSearch />}
              </ContentLayout>
            }
          />
        </>
      )}
    </Authenticator>
  );
}

function HomePlaceholder() {
  const flow = [
    {
      num: '1',
      title: 'Upload',
      body: '영상을 S3에 업로드합니다. 사용자별 격리된 저장공간이 제공됩니다.',
    },
    {
      num: '2',
      title: 'Pegasus Analyze',
      body: 'Pegasus 1.2 모델이 영상을 시청하고 자연어로 내용을 설명합니다. 커스텀 프롬프트로 원하는 질문을 할 수 있습니다.',
    },
    {
      num: '3',
      title: 'Marengo Embed (사전 인덱싱 완료)',
      body: '공용 샘플 영상(SHARED)에 대한 Visual + Audio 멀티모달 임베딩을 사전에 완료해 OpenSearch에 적재해두었습니다.',
    },
    {
      num: '4',
      title: 'Marengo Search',
      body: '자연어로 검색하면 쿼리를 벡터로 변환 후 OpenSearch에서 유사도 검색하여 매칭되는 영상 구간을 반환합니다.',
    },
  ];

  const features = [
    { icon: '🔒', title: 'Cognito 인증', body: '회원가입/로그인 기반 사용자 격리, JWT 토큰 기반 API 인가.' },
    { icon: '🧠', title: '영상 이해 (Pegasus)', body: '영상 전체를 분석하여 텍스트로 설명, 분석 히스토리 저장.' },
    { icon: '🔍', title: '벡터 임베딩 (Marengo)', body: '영상을 검색 가능한 벡터로 변환. 512차원, 10초 세그먼트.' },
    { icon: '🔎', title: '시맨틱 검색', body: '"격투 장면" 같은 자연어로 영상 구간 검색, 타임스탬프 반환.' },
    { icon: '📊', title: '일일 사용량 제한', body: 'Analyze 20회, Embed 10회, Search 100회/일. DDB 카운터 기반.' },
    { icon: '🌐', title: '서울 리전 배포', body: '모든 리소스가 ap-northeast-2에 배포. CDK IaC로 원클릭 배포.' },
  ];

  const techTags = ['React 18', 'TypeScript', 'AWS Amplify', 'Cognito', 'API Gateway', 'Lambda (Python)', 'DynamoDB', 'S3 + CloudFront', 'OpenSearch Serverless', 'Bedrock – Marengo 3.0', 'Bedrock – Pegasus 1.2', 'AWS CDK', 'Cloudscape Design'];

  return (
    <SpaceBetween size="l">
      <Container header={<Header variant="h2">Architecture</Header>}>
        <Box textAlign="center">
          <img src="/architecture.png" alt="Architecture" style={{ maxWidth: '100%' }} />
        </Box>
      </Container>

      <Container header={<Header variant="h2">How It Works</Header>}>
        <ColumnLayout columns={4} minColumnWidth={220}>
          {flow.map((f) => (
            <Box key={f.num}>
              <Box variant="awsui-key-label">Step {f.num}</Box>
              <Box variant="h3" padding={{ top: 'xxs', bottom: 'xs' }}>{f.title}</Box>
              <Box variant="p" color="text-body-secondary">{f.body}</Box>
            </Box>
          ))}
        </ColumnLayout>
      </Container>

      <Container header={<Header variant="h2">Features</Header>}>
        <ColumnLayout columns={3} minColumnWidth={240}>
          {features.map((f) => (
            <Box key={f.title}>
              <Box variant="h3" padding={{ bottom: 'xs' }}>{f.icon} {f.title}</Box>
              <Box variant="p" color="text-body-secondary">{f.body}</Box>
            </Box>
          ))}
        </ColumnLayout>
      </Container>

      <Container header={<Header variant="h2">Tech Stack</Header>}>
        <SpaceBetween direction="horizontal" size="xs">
          {techTags.map((t) => <Badge key={t}>{t}</Badge>)}
        </SpaceBetween>
      </Container>
    </SpaceBetween>
  );
}

export default App;
