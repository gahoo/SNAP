# SNAP is Not A Pipeline
SNAP基于jinja2模板，依据模版生成脚本以及流程的依赖关系。

## App
asnap程序用于处理与APP相关的命令。
### new
新建App模板及其目录结构

### build
根据模板与参数实例化生成脚本。

### node
生成workflow的节点
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

## 安装
```sh
git clone ssh://git@123.56.188.74:10022/lijiaping/SNAP.git
cd SNAP
virtualenv .env
source .env/bin/activate
pip install -r requirements.txt
```
## 使用
```sh
snap app new -name appname

snap app build -name appname
snap app build -config appname/config.yaml
snap app build -name appname -param appname/parameters.yaml -out appname.sh

snap app node -name appname -type all
snap app node -name appname -type app
snap app node -name appname -type load -node bam
```
## 与atom编辑器的搭配使用
1. 安装[script](https://atom.io/packages/script)
2. 编辑`~/.atom/packages/script/lib/grammars.coffee`，在末尾添加：
```coffee
  YAML:
    "File Based":
      command: "snap" #改成snap所在路径
      args: (context) -> ['app', 'build', '-config', context.filepath]
```

3.保存为`.yaml`后缀文件，`cmd ＋ i`即可生成预览脚本
> 自动补全：修改[~/.atom/snippets.cson](http://123.56.188.74:10080/gitlab/lijiaping/atom/blob/master/snippets.cson)
