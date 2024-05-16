import os
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Union, List

import polars as pl
from  googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dagster import IOManager, InputContext, OutputContext
from .minio_io_manager import connect_minio
from .. import constants


@contextmanager
def youtube_client(config: dict):
    api_service_name = config["api_service_name"]
    api_version = config["api_version"]
    api_key = config["api_key"]
    
    youtube = build(
        serviceName=api_service_name,
        version=api_version,
        developerKey=api_key
    )
    try:
        yield youtube
    except HttpError as e:
        raise 'An HTTP error %d occurred:\n%s' % (e.resp.status, e.content)
    
    
class YoutubeIOManager(IOManager):
    
    def __init__(self, config) -> None:
        self._config = config
        
        
    def _get_path(self, context: Union[InputContext, OutputContext]):
        
        start = constants.START_DATE
        end = constants.END_DATE
        start_date = datetime.strptime(start, "%Y-%m-%d")
        end_date = datetime.strptime(end, "%Y-%m-%d")

        layer, schema, table = context.asset_key.path
        table = "youtube_trending_data"
        layer = "bronze"
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        
        key_names: list[str] = []
        tmp_file_paths: list[str] = []
        
        for date in range((end_date - start_date).days + 1):
            partition_date = start_date + timedelta(days=date)
            partition_date.strftime("%Y-%m")
            key_name = f"{key}/" + str(partition_date)[:7].replace("-", "") + ".pq"
            # key_name = "bronze/youtube/youtube_trending_data/202011.pq"
            tmp_file_path = "/tmp/file-{}-{}.parquet".format(
                datetime.today().strftime("%Y%m%d%H%M%S"), 
                str(partition_date)[:7].replace("-", "")
            )
            # tmp_file_path = "/tmp/file-2020-11.parquet"
            context.log.info(f"INFO -> key_name: {key_name}")
            context.log.info(f"INFO -> tmp_file_path: {tmp_file_path}")
            
            key_names.append(key_name)
            tmp_file_paths.append(tmp_file_path)
            
        return key_names, tmp_file_paths
    
    
    def list_of_list(self, obj: pl.Series) -> list[list[str]]:
        start = 0
        end = 50
        lists: List[List] = []
        for lst in range(len(obj) // 50 + 1):
            lists.append(list(obj)[start:end])
            start += 50
            end += 50
        return lists
    
    
    def get_DataFrame(self, context, field: str) -> pl.DataFrame:
        bucket_name = self._config.get("bucket")
        key_names, tmp_file_paths = self._get_path(context)
        
        try:
            with connect_minio(self._config) as client:
                # Make bucket if not exist.
                found = client.bucket_exists(bucket_name)
                if not found:
                    client.make_bucket(bucket_name)
                else:
                    print(f"Bucket {bucket_name} already exists")
                    
        except Exception as e:
                raise e
        
        list_dfs: list[pl.DataFrame] = []
        for key_name, tmp_file_path in zip(key_names, tmp_file_paths):
            client.fget_object(bucket_name, key_name, tmp_file_path)
            df = pl.read_parquet(tmp_file_path)[field].unique()
            list_dfs.append(df)
            time.sleep(0.5)
            
            context.log.info(f"INFO -> key_name: {key_name}, tmp_file_path: {tmp_file_path}")
            os.remove(tmp_file_path)
            
        pl_data = pl.concat(list_dfs).unique()
        return pl_data
        
        
    def downLoad_videoCategories(self, context, obj: pl.DataFrame) -> pl.DataFrame:  
        
        # pl_data = self.get_DataFrame(context, "categoryId")
        pl_data = obj["categoryId"].unique()
        
        with youtube_client(self._config) as service:
            categoryNames: list[str] = []
            categoryIds: list[str] = []
            
            categoryId_list: pl.Series = pl_data
            context.log.info("Divide categoryIds to multiple list categoryIds")
            
            for categoryId in list(categoryId_list.unique()):
                request = service.videoCategories().list(
                    part="snippet",
                    id=categoryId
                )
                response = request.execute()
                
                try:
                    categoryIds.append(str(response["items"][0]["id"]))
                    categoryNames.append(str(response["items"][0]["snippet"]["title"]))
                except IndexError:
                    categoryNames.append(response["items"]["snippet"]["title"])
        
            
        return pl.DataFrame(
            {
                "categoryId": categoryIds, 
                "categoryName": categoryNames
            }
        )

    
    def downLoad_linkVideos(self, context, obj: pl.DataFrame) -> pl.DataFrame:
            
        pl_data = obj["video_id"].unique()
            
        with youtube_client(self._config) as service:
            link_videos: list[str] = []
            videoIds: list[str] = []
            
            video_id_list: pl.Series = pl_data
            context.log.info("Divide videoId to multiple list videoId")
            
            for videoId in self.list_of_list(video_id_list.unique()):
                # videoId = list(map(lambda id: id[1:-1], videoId))
                # context.log.info(",".join(videoId)[:20])
                request = service.videos().list(
                    part="player",
                    id=",".join(videoId)
                )
                response = request.execute()
                
                for data in response["items"]:
                    try:
                        videoIds.append(str(data["id"]))
                        link_videos.append(str(data["player"]["embedHtml"][40:74]))
                    except IndexError as e:
                        link_videos.append(response["items"]["snippet"]["title"])
                        raise e
            
        return pl.DataFrame(
            {
                "videoId": videoIds, 
                "link_video": link_videos
            }
        )
            
            
    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        pass
    
    
    def load_input(self, context: InputContext) -> pl.DataFrame:
        bucket_name = self._config.get("bucket")
        key_name, tmp_file_path = self._get_path(context)
        
        try:
            with connect_minio(self._config) as client:
                # Make bucket if not exist.
                found = client.bucket_exists(bucket_name)
                if not found:
                    client.make_bucket(bucket_name)
                else:
                    print(f"Bucket {bucket_name} already exists")
                    
                client.fget_object(bucket_name, key_name, tmp_file_path)
                pd_data = pl.read_parquet(tmp_file_path)
                return pd_data
            
        except Exception as e:
            raise e
    


