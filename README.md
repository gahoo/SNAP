# SNAP is Not A Pipeline
SNAP基于jinja2模板，依据模版生成脚本以及流程的依赖关系。

## App
asnap程序用于处理与APP相关的命令。
### new
新建App模板及其目录结构

### build
根据模板与参数实例化生成脚本。
### test
测试单个APP
### run
根据配置文件执行单个APP

## Pipeline
psnap程序用于处理与Pipeline相关的命令

### new
新建Pipeline及其目录结构

### build
根据项目配置实例化所有相关APP脚本
### test
测试Pipeline
### run
根据配置文件执行流程
### docker
生成聚道需要的images
