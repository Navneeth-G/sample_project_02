def k8_to_aws_s3_main(config_dict):
  farm_list = config_dict["farm_list"]
  file_path_template = config_dict["template_path"]
  s3_bucket = config_dict["s3_bucket_name"]
  index_name = config_dict["index_name"]
  s3_key_prefix = config_dict["s3_key"]
  aws_access_key = config_dict["aws_access_key"]
  aws_secret_key = config_dict["aws_secret_key"]
  timezone = config_dict["es_timezone"]
  temp_output_dir = config_dict["temp_output_dir"]
  


