#!/bin/bash
#_Main_script_to_run_the_weather_data_pipeline

#_Set_up_colors_for_output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'_#_No_Color

#_Load_environment_variables
if_[_-f_.env_];_then
____echo_-e_"${GREEN}Loading_environment_variables_from_.env_file...${NC}"
____export_$(grep_-v_'^#'_.env_|_xargs)
else
____echo_-e_"${RED}Error:_.env_file_not_found._Please_create_one_based_on_.env.example${NC}"
____exit_1
fi

#_Update_GOOGLE_APPLICATION_CREDENTIALS_to_use_local_path
if_[_-f_"creds/creds.json"_];_then
____export_GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/creds/creds.json"
____echo_-e_"${GREEN}Using_credentials_from_$(pwd)/creds/creds.json${NC}"
fi

#_Check_required_environment_variables
required_vars=("GCP_PROJECT_ID"_"OPENWEATHER_API_KEY")
for_var_in_"${required_vars[@]}";_do
____if_[_-z_"${!var}"_];_then
________echo_-e_"${RED}Error:_Required_environment_variable_$var_is_not_set${NC}"
________exit_1
____fi
done

#_Function_to_check_if_Docker_is_running
check_docker()_{
____if_!_docker_info_>_/dev/null_2>&1;_then
________echo_-e_"${RED}Error:_Docker_is_not_running._Please_start_Docker_and_try_again.${NC}"
________exit_1
____fi
}

#_Function_to_check_if_Docker_Compose_is_installed
check_docker_compose()_{
____if_!_command_-v_docker-compose_&>_/dev/null;_then
________echo_-e_"${RED}Error:_docker-compose_is_not_installed._Please_install_it_and_try_again.${NC}"
________exit_1
____fi
}

#_Function_to_check_GCP_credentials
check_gcp_credentials()_{
____if_[_!_-f_"creds/creds.json"_];_then
________echo_-e_"${RED}Error:_GCP_credentials_file_not_found_at_creds/creds.json${NC}"
________echo_-e_"${YELLOW}Please_place_your_GCP_service_account_key_file_at_creds/creds.json${NC}"
________exit_1
____fi
}

#_Function_to_create_necessary_directories
create_directories()_{
____echo_-e_"${GREEN}Creating_necessary_directories...${NC}"
____mkdir_-p_data/raw_data/processed_logs
}

#_Function_to_check_and_create_GCS_bucket
setup_gcs_bucket()_{
____echo_-e_"${GREEN}Checking_and_setting_up_GCS_bucket...${NC}"
____python3_setup_gcs.py_--credentials_"$(pwd)/creds/creds.json"
____if_[_$?_-ne_0_];_then
________echo_-e_"${RED}Error_setting_up_GCS_bucket._Please_check_the_logs.${NC}"
________exit_1
____fi
}

#_Function_to_start_the_services
start_services()_{
____echo_-e_"${GREEN}Starting_services_with_Docker_Compose...${NC}"
____docker-compose_up_-d
}

#_Function_to_stop_the_services
stop_services()_{
____echo_-e_"${GREEN}Stopping_services...${NC}"
____docker-compose_down
}

#_Function_to_show_logs
show_logs()_{
____echo_-e_"${GREEN}Showing_logs_for_service:_$1...${NC}"
____docker-compose_logs_-f_"$1"
}

#_Function_to_run_Terraform
run_terraform()_{
____echo_-e_"${GREEN}Running_Terraform_to_set_up_infrastructure...${NC}"
____cd_load
____#_Set_environment_variable_for_Terraform
____export_TF_VAR_project_id="$GCP_PROJECT_ID"
____export_TF_VAR_region="${GCP_REGION:-asia-southeast1}"
____export_TF_VAR_dataset_id="${BIGQUERY_DATASET:-weather_data}"
____export_TF_VAR_environment="${ENVIRONMENT:-dev}"
____
____terraform_init
____terraform_apply_-auto-approve
____cd_..
}

#_Function_to_run_data_collection
run_collection()_{
____echo_-e_"${GREEN}Running_data_collection...${NC}"
____docker-compose_exec_extract_python_-m_extract.weather_collector
}

#_Function_to_run_data_processing
run_processing()_{
____echo_-e_"${GREEN}Running_data_processing...${NC}"
____docker-compose_exec_spark-master_spark-submit_\
________--master_spark://spark-master:7077_\
________--packages_org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0_\
________/app/spark/streaming/weather_streaming_processing.py
}

#_Function_to_run_dashboard_setup
run_dashboard_setup()_{
____echo_-e_"${GREEN}Setting_up_dashboard_views...${NC}"
____docker-compose_exec_api_python_-m_dashboard.setup_views_python
}

#_Function_to_check_data_quality
check_data_quality()_{
____echo_-e_"${GREEN}Checking_data_quality...${NC}"
____docker-compose_exec_api_python_-m_dashboard.check_data
}

#_Function_to_show_help
show_help()_{
____echo_"Usage:_./run.sh_[command]"
____echo_""
____echo_"Commands:"
____echo_"__start_____________Start_all_services"
____echo_"__stop______________Stop_all_services"
____echo_"__restart___________Restart_all_services"
____echo_"__logs_[service]____Show_logs_for_a_specific_service"
____echo_"__setup-gcs_________Check_and_create_GCS_bucket_if_needed"
____echo_"__terraform_________Run_Terraform_to_set_up_infrastructure"
____echo_"__collect___________Run_data_collection"
____echo_"__process___________Run_data_processing"
____echo_"__dashboard_________Set_up_dashboard_views"
____echo_"__quality___________Check_data_quality"
____echo_"__help______________Show_this_help_message"
____echo_""
____echo_"Examples:"
____echo_"__./run.sh_start"
____echo_"__./run.sh_logs_api"
}

#_Main_script_logic
case_"$1"_in
____start)
________check_docker
________check_docker_compose
________check_gcp_credentials
________create_directories
________setup_gcs_bucket
________start_services
________;;
____stop)
________stop_services
________;;
____restart)
________stop_services
________start_services
________;;
____logs)
________if_[_-z_"$2"_];_then
____________echo_-e_"${RED}Error:_Please_specify_a_service_name${NC}"
____________echo_-e_"${YELLOW}Example:_./run.sh_logs_api${NC}"
____________exit_1
________fi
________show_logs_"$2"
________;;
____setup-gcs)
________check_gcp_credentials
________setup_gcs_bucket
________;;
____terraform)
________check_gcp_credentials
________run_terraform
________;;
____collect)
________run_collection
________;;
____process)
________run_processing
________;;
____dashboard)
________run_dashboard_setup
________;;
____quality)
________check_data_quality
________;;
____help|--help|-h)
________show_help
________;;
____*)
________echo_-e_"${RED}Error:_Unknown_command_'$1'${NC}"
________show_help
________exit_1
________;;
esac

exit_0
