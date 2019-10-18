SET FOREIGN_KEY_CHECKS=0;

INSERT INTO `job_definition` (`id`, `job_def_id`, `job_def_url`, `flow_definition_id`, `job_name`, `scheduler`, `username`, `created_ts`, `updated_ts`)
VALUES (1,'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_1&job=job_1',
'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_1&job=job_1',
1,'job_1','azkaban','metrics','2019-05-28 22:07:43','2019-05-28 22:07:43'),
(2,'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_3&job=testJob',
'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_3&job=testJob',3,
'testJob','azkaban','metrics','2019-05-29 13:13:03','2019-05-29 13:13:03');

INSERT INTO `tuning_job_definition` (`job_definition_id`, `client`, `tuning_algorithm_id`, `tuning_enabled`, `auto_apply`, `average_resource_usage`, `average_execution_time`, `average_input_size_in_bytes`, `allowed_max_resource_usage_percent`, `allowed_max_execution_time_percent`, `tuning_disabled_reason`, `number_of_iterations`, `created_ts`, `updated_ts`, `show_recommendation_count`, `tuning_re_enable_timestamp`, `tuning_re_enablement_count`) VALUES
(1,'azkaban',4,0,1,3920.58746667,43.52463333,8200657043456,150,150,'All Heuristics Passed',478,'2019-05-29 03:06:47','2019-10-03 03:12:44',90,null ,null),
(2,'azkaban',4,0,1,20.567,2.33,7043456,150,150,'All Heuristics Passed',50,'2019-08-09 03:06:47','2019-09-01 03:12:44',90,'2019-08-29 08:20:50' ,1);

INSERT INTO `job_execution` (`id`, `job_exec_id`, `job_exec_url`, `job_definition_id`, `flow_execution_id`, `execution_state`, `resource_usage`, `execution_time`, `input_size_in_bytes`, `auto_tuning_fault`, `created_ts`, `updated_ts`) VALUES
(3253,'https://elephant.linkedin.com:8443/executor?execid=6109371&job=job_1&attempt=0','https://elephant.linkedin.com:8443/executor?execid=6109371&job=job_1&attempt=0',
100150,2631,'SUCCEEDED',2408.440625,78.5789,13009439225586,0,'2019-10-02 22:29:31','2019-10-03 01:05:20'),
(3256,'https://elephant.linkedin.com:8443/executor?execid=6110605&job=job_1&attempt=0','https://elephant.linkedin.com:8443/executor?execid=6110605&job=job_1&attempt=0',
100150,2634,'SUCCEEDED',1913.4640296766493,55.812016666666665,13009439229729,0,'2019-10-03 01:29:49','2019-10-03 03:12:44'),
(3309,'https://elephant.linkedin.com:8443/executor?execid=6138232&job=job_2&attempt=0','https://elephant.linkedin.com:8443/executor?execid=6138232&job=job_2&attempt=0',
100150,2687,'SUCCEEDED',5529.082768554687,55.79035,13039741977613,0,'2019-10-05 19:29:07','2019-10-05 20:50:44'),
(3312,'https://elephant.linkedin.com:8443/executor?execid=6139323&job=job_1&attempt=0','https://elephant.linkedin.com:8443/executor?execid=6139323&job=job_1&attempt=0',
100150,2690,'FAILED',2.8376844618055554,12.343416666666666,7675538881,0,'2019-10-05 22:29:01','2019-10-05 23:24:02'),
(3315,'https://elephant.linkedin.com:8443/executor?execid=6140409&job=job_1&attempt=0','https://elephant.linkedin.com:8443/executor?execid=6140409&job=job_1&attempt=0',
100150,2693,'SUCCEEDED',8926.782855088975,57.69286666666667,13034716636256,0,'2019-10-06 01:29:02','2019-10-06 02:56:25'),
(3318,'https://elephant.linkedin.com:8443/executor?execid=6141465&job=job_1&attempt=0','https://elephant.linkedin.com:8443/executor?execid=6141465&job=job_1&attempt=0',
100150,2696,'SUCCEEDED',10556.260704752603,65.82218333333333,13035295645066,0,'2019-10-06 04:28:56','2019-10-06 06:14:50'),
(3321,'https://elephant.linkedin.com:8443/executor?execid=6142551&job=job_1&attempt=0','https://elephant.linkedin.com:8443/executor?execid=6142551&job=job_1&attempt=0',
100150,2699,'SUCCEEDED',4887.093991156684,67.32581666666667,13035784705279,0,'2019-10-06 07:28:57','2019-10-06 08:59:13'),
(3324,'https://elephant.linkedin.com:8443/executor?execid=123456&job=job_1&attempt=0','https://elephant.linkedin.com:8443/executor?execid=123456&job=job_1&attempt=0',
100151,2703,'SUCCEEDED',487.7801196289063,62.38863333333333,721397153615,0,'2019-10-06 13:00:12','2019-10-06 14:46:06'),
(3327,'https://elephant.linkedin.com:8443/executor?execid=6145169&job=job_1&attempt=0','https://elephant.linkedin.com:8443/executor?execid=6145169&job=job_1&attempt=0',
100150,2705,'SUCCEEDED',4233.963060709635,63.75875,13036607757773,0,'2019-10-06 13:29:01','2019-10-06 15:05:08'),
(3330,'https://elephant.linkedin.com:8443/executor?execid=6146530&job=job_1&attempt=0','https://elephant.linkedin.com:8443/executor?execid=6146530&job=job_1&attempt=0',
100150,2708,'FAILED',3.55447265625,13.879066666666667,7675539253,0,'2019-10-06 16:28:58','2019-10-06 18:14:33');



INSERT INTO `tuning_job_execution_param_set` (`job_suggested_param_set_id`, `job_execution_id`, `tuning_enabled`, `is_retried`, `created_ts`, `updated_ts`) VALUES
(2071,3309,0,0,'2019-10-05 19:29:07','2019-10-05 19:29:07'),(2074,3312,0,0,'2019-10-05 22:29:01','2019-10-05 22:29:01'),
(2074,3315,0,0,'2019-10-06 01:29:02','2019-10-06 01:29:02'),(2074,3318,0,0,'2019-10-06 04:28:56','2019-10-06 04:28:57'),
(2074,3321,0,0,'2019-10-06 07:28:57','2019-10-06 07:28:58'),(2074,3324,0,0,'2019-10-06 10:29:07','2019-10-06 10:29:07'),
(2074,3327,0,0,'2019-10-06 13:29:01','2019-10-06 13:29:01'),(2074,3330,0,0,'2019-10-06 16:28:58','2019-10-06 16:28:58'),
(2075,3253,1,0,'2019-10-02 22:29:31','2019-10-02 22:29:32'),(2076,3256,1,0,'2019-10-03 01:29:49','2019-10-03 01:29:49');

INSERT INTO `job_suggested_param_set` (`id`, `job_definition_id`, `tuning_algorithm_id`, `param_set_state`, `are_constraints_violated`, `is_param_set_default`, `is_param_set_best`, `is_manually_overriden_parameter`, `is_param_set_suggested`, `fitness`, `fitness_job_execution_id`, `created_ts`, `updated_ts`) VALUES
(1678,1,4,'FITNESS_COMPUTED',0,1,0,0,0,5378,1704,'2019-06-06 08:23:19','2019-06-06 14:50:18'),
(1680,1,4,'FITNESS_COMPUTED',0,0,0,0,1,10154,1707,'2019-06-06 14:50:17','2019-06-06 19:57:29'),
(1682,1,4,'FITNESS_COMPUTED',0,0,0,0,1,4867,1710,'2019-06-06 19:57:29','2019-06-07 05:20:48'),
(1688,1,4,'FITNESS_COMPUTED',0,0,0,0,1,4251,1714,'2019-07-23 09:23:24','2019-07-23 15:49:19'),
(2071,1,4,'FITNESS_COMPUTED',0,0,1,0,1,10022,2895,'2019-09-13 15:12:57','2019-10-14 11:17:27'),
(2074,2,4,'FITNESS_COMPUTED',0,0,1,0,1,0,3249,'2019-10-01 11:42:35','2019-10-14 09:49:10'),
(2075,2,4,'FITNESS_COMPUTED',0,0,0,0,1,57668,3253,'2019-10-02 19:43:28','2019-10-03 01:05:20'),
(2076,2,4,'FITNESS_COMPUTED',0,0,0,0,1,57668,3256,'2019-10-03 01:05:20','2019-10-03 03:12:44');