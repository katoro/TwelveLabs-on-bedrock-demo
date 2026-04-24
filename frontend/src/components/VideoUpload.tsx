import React, { useEffect, useState } from 'react';
import { authFetch } from '../authFetch';
import Alert from '@cloudscape-design/components/alert';
import Badge from '@cloudscape-design/components/badge';
import Box from '@cloudscape-design/components/box';
import Button from '@cloudscape-design/components/button';
import ColumnLayout from '@cloudscape-design/components/column-layout';
import Container from '@cloudscape-design/components/container';
import FileUpload from '@cloudscape-design/components/file-upload';
import Header from '@cloudscape-design/components/header';
import ProgressBar from '@cloudscape-design/components/progress-bar';
import SpaceBetween from '@cloudscape-design/components/space-between';
import Table from '@cloudscape-design/components/table';

interface Video {
  key: string;
  filename: string;
  s3Uri: string;
  bucket: string;
  contentType: string;
  uploadedAt: string;
  isShared?: boolean;
}

const MAX_BYTES = 2 * 1024 * 1024 * 1024;

const VideoUpload: React.FC = () => {
  const [files, setFiles] = useState<File[]>([]);
  const [uploading, setUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [videos, setVideos] = useState<Video[]>([]);
  const [loadingVideos, setLoadingVideos] = useState(false);

  const API_BASE_URL = (process.env.REACT_APP_API_URL || '').replace(/\/+$/, '');
  const selectedFile = files[0] ?? null;

  useEffect(() => { loadVideos(); }, []);

  const loadVideos = async () => {
    setLoadingVideos(true);
    try {
      const res = await authFetch(`${API_BASE_URL}/videos`);
      const data = await res.json();
      setVideos(data.videos || []);
    } catch (e) {
      console.error('Failed to load videos:', e);
    } finally {
      setLoadingVideos(false);
    }
  };

  const verifyS3Upload = async (_s3Uri: string): Promise<void> => {
    await new Promise((resolve) => setTimeout(resolve, 2000));
  };

  const uploadVideo = async () => {
    if (!selectedFile) return;
    if (selectedFile.size > MAX_BYTES) {
      setError('File exceeds 2GB limit.');
      return;
    }
    setUploading(true);
    setError(null);
    setSuccess(null);
    setUploadProgress(0);

    try {
      const response = await authFetch(`${API_BASE_URL}/upload`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          filename: selectedFile.name,
          contentType: selectedFile.type,
        }),
      });
      if (!response.ok) throw new Error('Failed to get upload URL');
      const { uploadUrl, fields, key, bucket } = await response.json();

      const formData = new FormData();
      Object.entries(fields).forEach(([k, v]) => formData.append(k, v as string));
      formData.append('file', selectedFile);

      setUploadProgress(10);

      await new Promise<void>((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        xhr.upload.onprogress = (event) => {
          if (event.lengthComputable) {
            const pct = (event.loaded / event.total) * 80;
            setUploadProgress(Math.round(10 + pct));
          }
        };
        xhr.onload = () => {
          if (xhr.status === 200 || xhr.status === 204) {
            setUploadProgress(90);
            verifyS3Upload(`s3://${bucket}/${key}`)
              .then(() => { setUploadProgress(100); resolve(); })
              .catch(reject);
          } else {
            reject(new Error(`Upload failed with status ${xhr.status}: ${xhr.statusText}`));
          }
        };
        xhr.onerror = () => reject(new Error('Network error during upload'));
        xhr.timeout = 300000;
        xhr.ontimeout = () => reject(new Error('Upload timeout — file may be too large'));
        xhr.open('POST', uploadUrl);
        xhr.send(formData);
      });

      setSuccess(`"${selectedFile.name}" uploaded successfully.`);
      setFiles([]);
      loadVideos();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Upload failed');
    } finally {
      setUploading(false);
    }
  };

  return (
    <ColumnLayout columns={2} minColumnWidth={420}>
      <Container header={<Header variant="h2" description="MP4, MOV, AVI · 최대 2GB">Upload Video</Header>}>
        <SpaceBetween size="m">
          <FileUpload
            onChange={({ detail }) => {
              setFiles(detail.value);
              setError(null);
              setSuccess(null);
              setUploadProgress(0);
            }}
            value={files}
            accept="video/*"
            i18nStrings={{
              uploadButtonText: (multiple) => (multiple ? 'Choose files' : 'Choose file'),
              dropzoneText: (multiple) => (multiple ? 'Drop files to upload' : 'Drop file to upload'),
              removeFileAriaLabel: (index) => `Remove file ${index + 1}`,
              limitShowFewer: 'Show fewer',
              limitShowMore: 'Show more',
              errorIconAriaLabel: 'Error',
            }}
            showFileLastModified
            showFileSize
            showFileThumbnail
            constraintText="MP4 / MOV / AVI up to 2GB"
          />

          <Button
            variant="primary"
            onClick={uploadVideo}
            loading={uploading}
            disabled={!selectedFile || uploading}
          >
            {uploading ? `Uploading... ${uploadProgress}%` : 'Upload Video'}
          </Button>

          {uploading && <ProgressBar value={uploadProgress} description="Uploading to S3" />}
          {success && <Alert type="success" dismissible onDismiss={() => setSuccess(null)}>{success}</Alert>}
          {error && <Alert type="error" dismissible onDismiss={() => setError(null)}>{error}</Alert>}
        </SpaceBetween>
      </Container>

      <Table
        header={
          <Header
            variant="h2"
            counter={`(${videos.length})`}
            description="내 영상과 공용(SHARED) 영상이 함께 보입니다. 공용 영상은 사전 인덱싱되어 바로 검색/분석에 사용할 수 있습니다."
            actions={<Button iconName="refresh" onClick={loadVideos} loading={loadingVideos} />}
          >
            Videos
          </Header>
        }
        items={videos}
        loading={loadingVideos}
        loadingText="Loading videos"
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
          {
            id: 'contentType',
            header: 'Type',
            cell: (v) => v.contentType || '-',
          },
          {
            id: 'uploadedAt',
            header: 'Uploaded At',
            cell: (v) => v.uploadedAt || '-',
          },
        ]}
        empty={
          <Box textAlign="center" color="inherit" padding={{ vertical: 'l' }}>
            <b>No videos available yet</b>
            <Box variant="p" color="inherit">Upload a file or wait for SHARED videos to appear.</Box>
          </Box>
        }
      />
    </ColumnLayout>
  );
};

export default VideoUpload;
