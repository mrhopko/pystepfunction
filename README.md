# pystepfunctions

Create AWS Stepfunction asl.json files (state machine definition files) using python.  
Pre-made tasks that are easy to use.  
Test dataflow through the stepfunction using the same python code.  
Visualise the stepfunction using pyvis for easier dubugging without the use of the AWS console.  

## Installation
```
pip3 install pystepfunctions
```

## Define a Stepfunction State Machine
```
from pystepfunction.tasks import LambdaTask, GlueTask, SucceedTask
from pystepfunction.branch import Branch


# create a simple chain of tasks
lambda_task = (
    LambdaTask(name="LambdaTaskName", function_arn="my-lambda-arn")
    .and_then(GlueTask(name="GlueTaskName", job_name="my-glue-job-name"))
    .and_then(SucceedTask(name="SucceedTaskName"))
)
# or
lambda_task = (
    LambdaTask(name="LambdaTaskName", function_arn="my-lambda-arn") 
    >> GlueTask(name="GlueTaskName", job_name="my-glue-job-name")
    >> SucceedTask(name="SucceedTaskName")
)

# create a branch
easy_branch = Branch(comment="This is an easy branch", start_task=lambda_task)
# view the asl as a dict
asl = easy_branch.to_asl()
asl

# write the asl to a file
asl.write_asl("my_asl_file.asl.json")
```

## Visualise the network
```
from pystepfunction.viz import BranchViz
BranchViz(easy_branch).show("easy_branch.html") 
```