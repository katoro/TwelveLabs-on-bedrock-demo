import React, { useRef, useEffect, useState } from 'react';
import { authFetch } from '../authFetch';

interface VideoPlayerProps {
  videoS3Uri: string;
  startTime?: number;
  autoPlay?: boolean;
  onError?: (error: string) => void;
  className?: string;
}

interface VideoUrlResponse {
  presignedUrl: string;
  videoS3Uri: string;
  bucket: string;
  key: string;
}

const VideoPlayer: React.FC<VideoPlayerProps> = ({
  videoS3Uri,
  startTime = 0,
  autoPlay = false,
  onError,
  className = ''
}) => {
  const videoRef = useRef<HTMLVideoElement>(null);
  const [videoUrl, setVideoUrl] = useState<string>('');
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string>('');

  useEffect(() => {
    const fetchVideoUrl = async () => {
      try {
        console.log(`🎥 VideoPlayer: Starting video URL fetch for S3 URI: ${videoS3Uri}`);
        console.log(`🌐 API URL: ${process.env.REACT_APP_API_URL}`);
        
        setLoading(true);
        setError('');
        
        const apiUrl = `${process.env.REACT_APP_API_URL}/video-url?videoS3Uri=${encodeURIComponent(videoS3Uri)}`;
        console.log(`📡 Making request to: ${apiUrl}`);
        
        const response = await authFetch(apiUrl);
        console.log(`📡 Response status: ${response.status} ${response.statusText}`);
        console.log(`📡 Response headers:`, Object.fromEntries(response.headers.entries()));
        
        if (!response.ok) {
          const errorData = await response.json().catch(() => ({ error: 'Unknown error' }));
          console.error(`❌ Failed to get video URL:`, errorData);
          throw new Error(errorData.error || 'Failed to get video URL');
        }
        
        const data: VideoUrlResponse = await response.json();
        console.log(`✅ Successfully received video URL response:`, {
          bucket: data.bucket,
          key: data.key,
          videoS3Uri: data.videoS3Uri,
          presignedUrlLength: data.presignedUrl?.length || 0,
          presignedUrlPreview: data.presignedUrl?.substring(0, 100) + '...'
        });
        
        setVideoUrl(data.presignedUrl);
        console.log(`🎬 Video URL set successfully, length: ${data.presignedUrl?.length}`);
        
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : 'Unknown error occurred';
        console.error(`❌ VideoPlayer error:`, err);
        setError(errorMessage);
        if (onError) {
          onError(errorMessage);
        }
      } finally {
        setLoading(false);
        console.log(`🏁 VideoPlayer: Finished loading process`);
      }
    };

    if (videoS3Uri) {
      console.log(`🚀 VideoPlayer: useEffect triggered with S3 URI: ${videoS3Uri}`);
      fetchVideoUrl();
    } else {
      console.warn(`⚠️ VideoPlayer: No videoS3Uri provided`);
    }
  }, [videoS3Uri]);

  useEffect(() => {
    // Set the video start time when URL is loaded
    if (videoRef.current && videoUrl && !loading) {
      const video = videoRef.current;
      console.log(`⏰ VideoPlayer: Setting up timestamp handling for startTime: ${startTime}`);
      console.log(`🎬 Video element ready, URL: ${videoUrl.substring(0, 50)}...`);
      
      const handleLoadedMetadata = () => {
        console.log(`📊 Video metadata loaded - Duration: ${video.duration}s, StartTime: ${startTime}s`);
        if (startTime > 0 && startTime < video.duration) {
          console.log(`⏭️ Setting video currentTime to: ${startTime}s`);
          video.currentTime = startTime;
        } else if (startTime > 0) {
          console.warn(`⚠️ StartTime ${startTime}s is greater than video duration ${video.duration}s`);
        }
      };

      const handleCanPlay = () => {
        console.log(`▶️ Video can start playing - readyState: ${video.readyState}`);
      };

      const handleError = (event: Event) => {
        console.error(`❌ Video element error:`, event);
        console.error(`❌ Video error details:`, {
          error: video.error,
          networkState: video.networkState,
          readyState: video.readyState,
          currentSrc: video.currentSrc
        });
      };

      video.addEventListener('loadedmetadata', handleLoadedMetadata);
      video.addEventListener('canplay', handleCanPlay);
      video.addEventListener('error', handleError);
      
      return () => {
        video.removeEventListener('loadedmetadata', handleLoadedMetadata);
        video.removeEventListener('canplay', handleCanPlay);
        video.removeEventListener('error', handleError);
      };
    }
  }, [videoUrl, startTime, loading]);

  const formatTime = (seconds: number): string => {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);
    
    if (hours > 0) {
      return `${hours}:${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
    } else {
      return `${minutes}:${secs.toString().padStart(2, '0')}`;
    }
  };

  if (loading) {
    return (
      <div className={`video-player-loading ${className}`}>
        <div className="loading-spinner">
          <div className="spinner"></div>
          <p>Loading video...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className={`video-player-error ${className}`}>
        <div className="error-message">
          <p>❌ Error loading video</p>
          <p className="error-details">{error}</p>
        </div>
      </div>
    );
  }

  return (
    <div className={`video-player ${className}`}>
      <video
        ref={videoRef}
        src={videoUrl}
        controls
        autoPlay={autoPlay}
        playsInline
        style={{
          width: '100%',
          maxWidth: '100%',
          height: 'auto',
          borderRadius: '8px',
          backgroundColor: '#000'
        }}
        onError={(event) => {
          console.error(`❌ Video onError event triggered:`, event);
          const videoElement = event.currentTarget as HTMLVideoElement;
          const errorDetails = {
            error: videoElement.error,
            networkState: videoElement.networkState,
            readyState: videoElement.readyState,
            currentSrc: videoElement.currentSrc,
            videoUrl: videoUrl
          };
          console.error(`❌ Video error details:`, errorDetails);
          setError(`Failed to load video file: ${videoElement.error?.message || 'Unknown error'}`);
          if (onError) {
            onError(`Failed to load video file: ${videoElement.error?.message || 'Unknown error'}`);
          }
        }}
      >
        Your browser does not support the video tag.
      </video>
      
      {startTime > 0 && (
        <div className="video-info">
          <p className="start-time-info">
            ▶️ Starting at {formatTime(startTime)}
          </p>
        </div>
      )}
      
      <style>{`
        .video-player-loading,
        .video-player-error {
          width: 100%;
          min-height: 200px;
          display: flex;
          align-items: center;
          justify-content: center;
          background: #f7fafc;
          border: 1px solid #e2e8f0;
          border-radius: 8px;
        }
        
        .loading-spinner {
          text-align: center;
          color: #4a5568;
        }
        
        .spinner {
          border: 4px solid #e2e8f0;
          border-top: 4px solid #3182ce;
          border-radius: 50%;
          width: 40px;
          height: 40px;
          animation: spin 1s linear infinite;
          margin: 0 auto 10px auto;
        }
        
        @keyframes spin {
          0% { transform: rotate(0deg); }
          100% { transform: rotate(360deg); }
        }
        
        .error-message {
          text-align: center;
          color: #e53e3e;
        }
        
        .error-details {
          font-size: 14px;
          color: #718096;
          margin-top: 5px;
        }
        
        .video-info {
          background: #edf2f7;
          padding: 8px 12px;
          border-radius: 6px;
          margin-top: 8px;
          border-left: 4px solid #3182ce;
        }
        
        .start-time-info {
          margin: 0;
          font-size: 14px;
          color: #4a5568;
          font-weight: 500;
        }
      `}</style>
    </div>
  );
};

export default VideoPlayer;