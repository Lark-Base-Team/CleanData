from baseopensdk.api.base.v1 import *
from baseopensdk import BaseClient
import os, json, time


## 批量删除数据 ##
def batch_delete_data_func(data: str):

  # print(data, flush=True)
  data_json = json.loads(data)

  APP_TOKEN = data_json.get("APP_TOKEN")
  PERSONAL_BASE_TOKEN = data_json.get("PERSONAL_BASE_TOKEN")
  TABLE_ID = data_json.get("TABLE_ID")
  VIEW_ID = data_json.get("VIEW_ID")
  FIELD_NAME_LIST = data_json.get("FIELD_NAME_LIST")

  # print(APP_TOKEN)
  # print(PERSONAL_BASE_TOKEN)
  # print(TABLE_ID)
  # print(VIEW_ID)
  # print(FIELD_NAME_LIST)

  # 1. 构建client
  client: BaseClient = BaseClient.builder() \
      .app_token(APP_TOKEN) \
      .personal_base_token(PERSONAL_BASE_TOKEN) \
      .build()

  # 2. 遍历记录
  has_more = True
  page_token = ""

  list_view_request = ListAppTableViewRequest.builder() \
    .page_size(500) \
    .table_id(TABLE_ID) \
    .page_token(page_token) \
    .build()
  
  list_view_response = client.base.v1.app_table_view.list(
    list_view_request)
  
  view_list = getattr(list_view_response.data, 'items', [])
  # print(view_list)

  VIEW_NAME = ""
  for view_item in view_list:
    if view_item.view_id == VIEW_ID:
      # print(view_item.__dict__)
      # print(view_item.view_name, view_item.view_id)
      VIEW_NAME = view_item.view_name
      break
  
  record_id_list_all = []
  while has_more:
    list_record_request = ListAppTableRecordRequest.builder() \
      .page_size(500) \
      .view_id(VIEW_ID) \
      .filter("") \
      .table_id(TABLE_ID) \
      .page_token(page_token) \
      .build()

    
    data_flag = True
    count = 1
    while data_flag:
      if count == 10:
        return "获取数据失败，请求次数达到 10 次"
      
      list_record_response = client.base.v1.app_table_record.list(
          list_record_request)
      if list_record_response.data is None:
        time.sleep(2)
        count = count + 1
      else:
        data_flag = False

    # print(1, count)

    # print(list_record_response.__dict__)
    # print(list_record_response.data.__dict__)

    records = getattr(list_record_response.data, 'items', [])

    has_more = list_record_response.data.has_more
    page_token = list_record_response.data.page_token
    total = list_record_response.data.total
    result = '成功删除 ' + str(total) + ' 条数据'

    # # print(list_record_response.data.__dict__)
    # print(has_more)
    # print(page_token)
    # print(total)
    # print("=" * 30)

    if total == 0:
      return "当前视图【{}】中没有数据".format(VIEW_NAME)

    # 如果字段列表为空时，执行清空整个视图的数据
    if len(FIELD_NAME_LIST) == 0:
      record_id_list = []
      for record in records:
        # print(record.__dict__, flush=True)
        # print("=" * 30)
        record_id_list.append(record.record_id)

      # print(record_id_list)

      record_id_list_all.append(record_id_list)

      if not has_more:
        # print(record_id_list_all)
        for record_id_list in record_id_list_all:

          # 3. 删除视图中的全部数据
          batch_delete_records_request = BatchDeleteAppTableRecordRequest().builder() \
          .app_token(APP_TOKEN) \
          .table_id(TABLE_ID) \
          .request_body(
            BatchDeleteAppTableRecordRequestBody.builder() \
              .records(record_id_list) \
              .build()
          ) \
          .build()

          ###########################
          delete_flag = True
          count = 1
          while delete_flag:
            if count == 10:
              return "批量删除数据失败，请求次数达到 10 次"
            
            batch_update_records_response = client.base.v1.app_table_record.batch_delete(
                batch_delete_records_request)
            
            # print(batch_update_records_response.__dict__)

            if batch_update_records_response.code == 0:
              delete_flag = False
              break

            else:
              time.sleep(2)
              count = count + 1

          # print(2, count)

    # 如果字段列表不为空时，将清空字段列表中的字段数据
    else:
      record_list = []
      for record in records:
        # print(record.__dict__, flush=True)
        # print("=" * 30)
        fields_list = {}
        for field_name in FIELD_NAME_LIST:
          fields_list[field_name] = None

        record_list.append({
            "record_id": record.record_id,
            "fields": fields_list
        })
        
      # print(record_list)

      # 3. 更新字段的数据为空
      batch_update_records_request = BatchUpdateAppTableRecordRequest().builder() \
      .table_id(TABLE_ID) \
      .request_body(
        BatchUpdateAppTableRecordRequestBody.builder() \
          .records(record_list) \
          .build()
      ) \
      .build()

      #################
      batch_update_records_response = client.base.v1.app_table_record.batch_update(
          batch_update_records_request)
      
      result = '成功清除 ' + str(total) + ' 条数据中 ' + str(FIELD_NAME_LIST) + ' ' + str(len(FIELD_NAME_LIST)) + ' 个字段的数据'
      

  # print(result, flush=True)
  return result

