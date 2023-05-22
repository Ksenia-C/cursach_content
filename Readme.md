Получить сгенерированный датасет как он распологался относительно данного репозитория
```bash
unzip dataset_tree_incr.zip -d ../tree_incr
unzip dataset_tree_decr.zip -d ../tree_decr
unzip dataset_other.zip -d ../other
```

```workflow.sh``` - процесс генерации, начиная с исходных архивов batch_instance.csv и batch_task.csv по пути ```../datasets/``` относительно данного репозитория

```stat_workflow.sh``` + ```draw_graphs_for_article.ipynb``` - рисование статистик для сравнения сгенерированных и настоящих данных

