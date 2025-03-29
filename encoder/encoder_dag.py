"""
Video Encoding DAG using TaskFlow API

This DAG performs video encoding with multiple CRF and QP parameter combinations.
It processes a source video into multiple output versions with different quality settings.
"""

import os
import shutil
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

# Default settings
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Configuration (these could be stored in Airflow Variables)
WORKING_DIR = Variable.get("VIDEO_ENCODING_WORKING_DIR", "/tmp/video_encoding")
OUTPUT_DIR = Variable.get("VIDEO_ENCODING_OUTPUT_DIR", "/output/encoded_videos")
S3_BUCKET = Variable.get("VIDEO_ENCODING_S3_BUCKET", "my-encoded-videos")

# CRF values (lower = higher quality, higher bitrate)
CRF_VALUES = [18, 23, 28]

# QP values (lower = higher quality, higher bitrate)
QP_VALUES = [0, 10, 20, 30]

# Video presets to use
PRESETS = ["ultrafast", "medium", "veryslow"]


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=['video', 'encoding', 'ffmpeg'],
    description='Encode videos with multiple CRF and QP combinations using FFmpeg',
)
def video_encoding_taskflow():
    """
    DAG for encoding videos with multiple CRF and QP parameters.

    This DAG follows these steps:
    1. Validate and prepare the input video
    2. Generate parameter combinations
    3. Encode the video with each parameter combination
    4. Upload the encoded videos to storage
    5. Cleanup working files
    """

    @task
    def prepare_input_video(input_path: str) -> str:
        """
        Validates and prepares the input video for encoding.

        Args:
            input_path: Path to the input video file

        Returns:
            Path to the prepared input video file
        """
        import subprocess

        # Create working directory if it doesn't exist
        os.makedirs(WORKING_DIR, exist_ok=True)

        # Validate input file exists
        if not os.path.isfile(input_path):
            raise FileNotFoundError(f"Input video file not found: {input_path}")

        # Get video information with ffprobe
        probe_cmd = [
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=width,height,codec_name,duration",
            "-of", "json", input_path
        ]

        result = subprocess.run(
            probe_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )

        if result.returncode != 0:
            raise ValueError(f"Error analyzing input video: {result.stderr}")

        # Copy input file to working directory
        filename = os.path.basename(input_path)
        prepared_path = os.path.join(WORKING_DIR, f"input_{filename}")
        shutil.copy2(input_path, prepared_path)

        return prepared_path

    @task
    def generate_encoding_params() -> list[dict[str, int]]:
        """
        Generate all combinations of encoding parameters to use.

        Returns:
            list of parameter dictionaries with CRF and QP values
        """
        param_combinations = []

        for crf in CRF_VALUES:
            for qp in QP_VALUES:
                for preset in PRESETS:
                    param_combinations.append(
                        {
                            'crf': crf,
                            'qp': qp,
                            'preset': preset
                        }
                    )

        return param_combinations

    @task
    def encode_video(input_path: str, params: dict[str, int]) -> str:
        """
        Encode a single video with the given parameters using ffmpeg.

        Args:
            input_path: Path to the input video
            params: dictionary containing CRF and QP values

        Returns:
            Path to the encoded output video
        """
        import subprocess

        # Extract parameters
        crf = params['crf']
        qp = params['qp']
        preset = params['preset']

        # Create output filename based on parameters
        input_filename = os.path.basename(input_path)
        base_name = os.path.splitext(input_filename)[0]
        output_filename = f"{base_name}_crf{crf}_qp{qp}_{preset}.mp4"
        output_path = os.path.join(WORKING_DIR, output_filename)

        # Build ffmpeg command
        cmd = [
            "ffmpeg",
            "-i", input_path,
            "-c:v", "libx264",
            "-preset", preset,
            "-crf", str(crf),
            "-qp", str(qp),
            "-c:a", "aac",
            "-b:a", "128k",
            "-y",  # Overwrite output files without asking
            output_path
        ]

        # Run ffmpeg
        process = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )

        if process.returncode != 0:
            raise RuntimeError(f"FFmpeg encoding failed: {process.stderr}")

        return output_path

    @task
    def upload_video(encoded_path: str, params: dict[str, int]) -> dict[str, str]:
        """
        Upload an encoded video to cloud storage.

        Args:
            encoded_path: Path to the encoded video file
            params: The encoding parameters used

        Returns:
            dictionary with upload details
        """
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        # Create a descriptive key for S3
        filename = os.path.basename(encoded_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"encoded/{timestamp}/{filename}"

        # Upload to S3
        s3_hook = S3Hook()
        s3_hook.load_file(
            filename=encoded_path,
            key=s3_key,
            bucket_name=S3_BUCKET,
            replace=True
        )

        # Create permanent copy in output directory
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        output_path = os.path.join(OUTPUT_DIR, filename)
        shutil.copy2(encoded_path, output_path)

        return {
            "local_path": output_path,
            "s3_bucket": S3_BUCKET,
            "s3_key": s3_key,
            "params": params
        }

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def cleanup_working_files(input_path: str, encoded_paths: list[str]) -> None:
        """
        Clean up working files after encoding is complete.

        Args:
            input_path: Path to the input video
            encoded_paths: list of paths to encoded videos
        """
        # Remove input file copy
        if os.path.exists(input_path):
            os.remove(input_path)

        # Remove encoded files from working directory
        for path in encoded_paths:
            if os.path.exists(path):
                os.remove(path)

        # Optionally, remove the working directory if it's empty
        if os.path.exists(WORKING_DIR) and not os.listdir(WORKING_DIR):
            os.rmdir(WORKING_DIR)

    @task
    def generate_encoding_report(upload_results: list[dict[str, str]]) -> dict[str, list]:
        """
        Generate a report of all encoded videos.

        Args:
            upload_results: list of upload result dictionaries

        Returns:
            Report dictionary
        """
        report = {
            "total_videos_encoded": len(upload_results),
            "encoding_parameters": [],
            "s3_locations": []
        }

        for result in upload_results:
            report["encoding_parameters"].append(
                {
                    "filename": os.path.basename(result["local_path"]),
                    "crf": result["params"]["crf"],
                    "qp": result["params"]["qp"],
                    "preset": result["params"]["preset"]
                }
            )

            report["s3_locations"].append(f"s3://{result['s3_bucket']}/{result['s3_key']}")

        # You could store this report in a database or file for future reference
        return report

    # Define the main workflow
    # 1. Get today's input video path (this could come from a sensor or another task)
    input_video_path = Variable.get(
        "VIDEO_ENCODING_INPUT_PATH",
        "/input/sample_video.mp4"
    )

    # 2. Prepare the input video
    prepared_input = prepare_input_video(input_video_path)

    # 3. Generate all parameter combinations
    params_list = generate_encoding_params()

    # 4. Encode the video with each parameter combination
    encoded_videos = []
    for params in params_list:
        encoded_path = encode_video(prepared_input, params)
        encoded_videos.append((encoded_path, params))

    # 5. Upload each encoded video and collect results
    upload_results = []
    for encoded_path, params in encoded_videos:
        result = upload_video(encoded_path, params)
        upload_results.append(result)

    # 6. Generate encoding report
    report = generate_encoding_report(upload_results)

    # 7. Clean up working files
    cleanup_working_files(
        prepared_input,
        [path for path, _ in encoded_videos]
    )


# Create the DAG
video_encoding_dag = video_encoding_taskflow()
