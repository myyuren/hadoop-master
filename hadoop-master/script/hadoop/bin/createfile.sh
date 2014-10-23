#!/bin/bash

count=100000
data_path=/run/media/zhangbo/DISKA/农信hadoop数据/

head -n $count $data_path/gcassuremain.del > data/gcassuremain.del
head -n $count $data_path/miloancard.del > data/miloancard.del
head -n $count $data_path/gcassuremulticlient.del > data/gcassuremulticlient.del
head -n $count $data_path/employee.del > data/employee.del
head -n $count $data_path/organization.del > data/organization.del
head -n $count $data_path/param_class.del > data/param_class.del
head -n $count $data_path/pmparamrelation.del > data/pmparamrelation.del
head -n $count $data_path/XCRMS_GROUPRELATION.del > data/XCRMS_GROUPRELATION.del

scp data/*.del root@192.168.0.4:/root/hadoop/data/

