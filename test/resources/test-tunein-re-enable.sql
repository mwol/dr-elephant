INSERT INTO `flow_definition` (`id`, `flow_def_id`, `flow_def_url`, `created_ts`, `updated_ts`) VALUES
(1,'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_1',
'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_1','2019-05-28 22:07:43','2019-05-28 22:07:43'),
(2,'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_2',
'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_2','2019-05-28 23:07:54','2019-05-28 23:07:54'),
 (3,'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_3',
 'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_3','2019-05-29 13:13:03','2019-05-29 13:13:03');

INSERT INTO `job_definition` (`id`, `job_def_id`, `job_def_url`, `flow_definition_id`, `job_name`, `scheduler`, `username`, `created_ts`, `updated_ts`)
VALUES (100149,'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_1&job=job_1',
'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_1&job=job_1',
1,'job_1','azkaban','metrics','2019-05-28 22:07:43','2019-05-28 22:07:43'),

(100150,'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_3&job=job_3',
'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_3&job=job_3',
2,'job_3','azkaban','metrics','2019-06-28 03:09:54','2019-06-28 03:09:54'),

(100151,'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_3&job=testJob',
'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_3&job=testJob',3,
'testJob','azkaban','metrics','2019-05-29 13:13:03','2019-05-29 13:13:03');


INSERT INTO `tuning_algorithm` (`job_type`, `optimization_algo`, `optimization_algo_version`, `optimization_metric`, `created_ts`,
 `updated_ts`) VALUES
 ('PIG','PSO',1,'RESOURCE','2018-08-23 08:53:51','2018-08-23 08:53:51'),
 ('PIG','PSO_IPSO',3,'RESOURCE','2018-08-23 08:53:53','2018-08-23 08:53:53'),
 ('PIG','HBT',4,'RESOURCE','2018-08-29 13:16:25','2018-08-29 13:16:25'),
 ('SPARK','HBT',1,'RESOURCE','2018-09-04 07:26:29','2018-09-04 07:26:29');

INSERT INTO `tuning_job_definition` (`job_definition_id`, `client`, `tuning_algorithm_id`, `tuning_enabled`, `auto_apply`,
 `average_resource_usage`, `average_execution_time`, `average_input_size_in_bytes`, `allowed_max_resource_usage_percent`,
 `allowed_max_execution_time_percent`, `tuning_disabled_reason`, `number_of_iterations`, `created_ts`, `updated_ts`,
 `show_recommendation_count`, `tuning_re_enable_timestamp`, `tuning_re_enablement_count`) VALUES
 (100149,'azkaban',4,0,0,14.48426667,42.86533333,52739178496,150,150,'All Heuristics Passed',10,'2019-05-29 04:03:38',
 '2019-09-12 18:38:46',9, null, null),
 (100150,'azkaban',4,0,1,3920.58746667,43.52463333,8200657043456,150,150,'All Heuristics Passed',10,
'2019-05-29 03:06:47','2019-09-13 19:50:03',31, null, null),
(100151,'azkaban',4,0,1,202.07776667,30.99123333,591484944384,150,150,'All Heuristics Passed',10,
'2019-05-29 13:13:03','2019-09-13 18:27:26',4, null, null);

INSERT INTO `flow_execution` (`id`, `flow_exec_id`, `flow_exec_url`, `flow_definition_id`, `created_ts`, `updated_ts`) VALUES
(1573,'https://elephant.linkedin.com:8443/executor?execid=5252291',
'https://elephant.linkedin.com:8443/executor?execid=5252291',2,'2019-05-29 03:06:48','2019-05-29 03:06:48'),
(1574,'https://elephant.linkedin.com:8443/executor?execid=5252449',
'https://elephant.linkedin.com:8443/executor?execid=5252449',1,'2019-05-29 04:03:38','2019-05-29 04:03:39'),
(1575,'https://elephant.linkedin.com:8443/executor?execid=5252941',
'https://elephant.linkedin.com:8443/executor?execid=5252941',1,'2019-05-29 07:00:43','2019-05-29 07:00:44'),
(1576,'https://elephant.linkedin.com:8443/executor?execid=5253039',
'https://elephant.linkedin.com:8443/executor?execid=5253039',2,'2019-05-29 07:32:46','2019-05-29 07:32:47'),
(1590,'https://elephant.linkedin.com:8443/executor?execid=5257237',
'https://elephant.linkedin.com:8443/executor?execid=5257237',2,'2019-05-30 01:38:28','2019-05-30 01:38:29'),
(1595,'https://elephant.linkedin.com:8443/executor?execid=5258473',
'https://elephant.linkedin.com:8443/executor?execid=5258473',2,'2019-05-30 07:30:25','2019-05-30 07:30:25'),
(2279,'https://elephant.linkedin.com:8443/executor?execid=5947701',
'https://elephant.linkedin.com:8443/executor?execid=5947701',3,'2019-09-13 13:00:07','2019-09-13 13:00:08'),
(2280,'https://elephant.linkedin.com:8443/executor?execid=5948816',
'https://elephant.linkedin.com:8443/executor?execid=5948816',3,'2019-09-13 16:02:30','2019-09-13 16:02:31'),
(2283,'https://elephant.linkedin.com:8443/executor?execid=5950173',
'https://elephant.linkedin.com:8443/executor?execid=5950173',3,'2019-09-13 19:05:08','2019-09-13 19:05:09');

INSERT INTO `job_execution`
(`id`, `job_exec_id`, `job_exec_url`, `job_definition_id`, `flow_execution_id`, `execution_state`, `resource_usage`,
 `execution_time`, `input_size_in_bytes`, `auto_tuning_fault`, `created_ts`, `updated_ts`) VALUES
 (1573,'https://elephant.linkedin.com:8443/executor?execid=5252291&job=job_2&attempt=0',
 'https://elephant.linkedin.com:8443/executor?execid=5252291&job=job_2&attempt=0',100150,
 1573,'SUCCEEDED',6712.747777777778,35.98485,0,0,'2019-05-29 03:06:48','2019-05-29 05:13:22'),
 (1574,'https://elephant.linkedin.com:8443/executor?execid=5252449&job=job_1&attempt=0',
 'https://elephant.linkedin.com:8443/executor?execid=5252449&job=job_1&attempt=0',100149,
 1574,'SUCCEEDED',16.90162326388889,44.608583333333335,0,0,'2019-05-29 04:03:38','2019-05-29 06:16:26'),
 (1575,'https://elephant.linkedin.com:8443/executor?execid=5252941&job=job_1&attempt=0',
 'https://elephant.linkedin.com:8443/executor?execid=5252941&job=job_1&attempt=0',100149,
 1575,'SUCCEEDED',16.791666666666668,49.040533333333336,0,0,'2019-05-29 07:00:43','2019-05-29 09:11:44'),
 (1576,'https://elephant.linkedin.com:8443/executor?execid=5253039&job=job_2&attempt=0',
 'https://elephant.linkedin.com:8443/executor?execid=5253039&job=job_2&attempt=0',100150,
 1576,'SUCCEEDED',5378.952222222222,74.86451666666666,0,0,'2019-05-29 07:32:46','2019-05-29 10:07:47'),
 (1590,'https://elephant.linkedin.com:8443/executor?execid=5257237&job=job_2&attempt=0',
 'https://elephant.linkedin.com:8443/executor?execid=5257237&job=job_2&attempt=0',100150,
 1590,'SUCCEEDED',3540.8080555555557,114.48411666666667,0,0,'2019-05-30 01:38:28','2019-05-30 05:06:20'),
 (1595,'https://elephant.linkedin.com:8443/executor?execid=5258473&job=job_2&attempt=0',
 'https://elephant.linkedin.com:8443/executor?execid=5258473&job=job_2&attempt=0',100150,
 1595,'SUCCEEDED',3700.197222222222,81.27736666666667,0,0,'2019-05-30 07:30:25','2019-05-30 10:31:07'),
 (2661,'https://elephant.linkedin.com:8443/executor?execid=5947701&job=testJob&attempt=0',
 'https://elephant.linkedin.com:8443/executor?execid=5947701&job=testJob&attempt=0',100151,
 2279,'SUCCEEDED',188.92555555555555,44.67446666666667,341187933842,0,'2019-09-13 13:00:07','2019-09-13 15:43:21'),
 (2665,'https://elephant.linkedin.com:8443/executor?execid=5948816&job=testJob&attempt=0',
 'https://elephant.linkedin.com:8443/executor?execid=5948816&job=testJob&attempt=0',100151,
 2280,'SUCCEEDED',0,0,1,0,'2019-09-13 16:04:00','2019-09-13 19:41:21'),
 (2667,'https://elephant.linkedin.com:8443/executor?execid=5948816&job=testJob&attempt=0',
 'https://elephant.linkedin.com:8443/executor?execid=5948816&job=testJob&attempt=0',100151,
 2280,'SUCCEEDED',0,0,1,0,'2019-09-13 16:04:30','2019-09-13 19:41:51'),
  (2671,'https://elephant.linkedin.com:8443/executor?execid=5950173&job=testJob&attempt=0',
  'https://elephant.linkedin.com:8443/executor?execid=5950173&job=testJob&attempt=0',100151,
  2283,'SUCCEEDED',0,0,1,0,'2019-09-13 19:05:10','2019-09-13 23:52:02');

INSERT INTO `job_suggested_param_set` (`id`, `job_definition_id`, `tuning_algorithm_id`, `param_set_state`, `are_constraints_violated`,
`is_param_set_default`, `is_param_set_best`, `is_manually_overriden_parameter`, `is_param_set_suggested`, `fitness`, `fitness_job_execution_id`,
 `created_ts`, `updated_ts`) VALUES
  (1579,100149,3,'FITNESS_COMPUTED',0,1,0,0,0,0,1574,'2019-05-29 04:03:38','2019-09-15 11:20:17'),
  (1581,100149,3,'FITNESS_COMPUTED',0,0,1,0,1,112,1575,'2019-05-29 06:16:25','2019-05-29 09:11:44'),
  (1578,100150,3,'FITNESS_COMPUTED',0,1,0,0,0,149702,1573,'2019-05-29 03:06:48','2019-05-29 11:48:04'),
  (1580,100150,3,'FITNESS_COMPUTED',0,0,1,0,1,52588,1576,'2019-05-29 05:13:21','2019-05-29 17:08:44'),
  (1585,100150,3,'FITNESS_COMPUTED',0,0,0,0,1,49968,1595,'2019-05-29 12:33:20','2019-09-12 16:28:58'),
  (1586,100151,4,'FITNESS_COMPUTED',0,1,0,0,0,10042,2261,'2019-05-29 13:13:03','2019-05-29 15:05:03'),
  (1587,100151,4,'FITNESS_COMPUTED',0,0,0,0,1,5615,2665,'2019-05-29 15:09:46','2019-05-29 16:10:45'),
  (1588,100151,4,'FITNESS_COMPUTED',0,0,0,0,1,0,2667,'2019-05-29 15:59:30','2019-05-29 19:02:58'),
  (1589,100151,4,'FITNESS_COMPUTED',0,0,0,0,1,10342,2671,'2019-05-29 16:10:45','2019-05-29 18:12:54');

INSERT INTO `tuning_job_execution_param_set` (`job_suggested_param_set_id`, `job_execution_id`, `tuning_enabled`, `is_retried`, `created_ts`, `updated_ts`) VALUES
(1585,1590,0,0,'2019-05-30 01:38:28','2019-05-30 01:38:29'),
(1585,1595,0,0,'2019-05-30 07:30:25','2019-05-30 07:30:25'),
(1578,1573,1,0,'2019-05-29 03:06:48','2019-05-29 03:06:48'),
(1580,1576,1,0,'2019-05-29 07:32:46','2019-05-29 07:32:47'),
(1586,2661,1,0,'2019-09-13 13:00:07','2019-09-13 13:00:08'),
(1587,2665,1,0,'2019-09-13 16:07:07','2019-09-13 16:07:08'),
(1588,2667,1,0,'2019-09-13 16:09:06','2019-09-13 16:09:07'),
(1589,2671,0,0,'2019-09-13 19:12:19','2019-09-13 19:12:19');