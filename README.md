# SNAP is Not A Pipeline
基于模板的流程创建、测试、运行管理工具。

## 安装
[![asciicast](https://asciinema.org/a/rmZzvA80wSKQJgWbLR9TAL7kh.png)](https://asciinema.org/a/rmZzvA80wSKQJgWbLR9TAL7kh)
```bash
git clone ssh://git@gitlab.igenecode.com:10022/lijiaping/SNAP.git
cd SNAP
virtualenv .env
source .env/bin/activate
pip install -r requirements.txt
```

详细安装及使用方法请参考[Wiki](wikis)

## 与atom编辑器的搭配使用
1. 安装[script](https://atom.io/packages/script)
2. 编辑`~/.atom/packages/script/lib/grammars.coffee`，在末尾添加：
```coffee
  YAML:
    "File Based":
      command: "snap" #改成snap所在路径
      args: (context) -> ['app', 'build', '-config', context.filepath]
```

3. 保存为`.yaml`后缀文件，`cmd ＋ i`即可生成预览脚本
> 自动补全：修改[~/.atom/snippets.cson](http://gitlab.igenecode.com:10080/lijiaping/atom/blob/master/snippets.cson)
