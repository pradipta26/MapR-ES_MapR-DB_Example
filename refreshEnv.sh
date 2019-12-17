maprcli table delete -path /apps/1MBDA/1CAT/itemTable
hadoop fs -rm -r /user/1MBDA/1CAT/itemPOC/input/itemDetails/maprFS_checkpoint/
hadoop fs -mkdir -p /user/1MBDA/1CAT/itemPOC/input/itemDetails/maprFS_checkpoint
maprcli table create -path /apps/1MBDA/1CAT/itemTable -tabletype json -defaultreadperm p -defaultwriteperm p
maprcli stream topic list -path /apps/1MBDA/1CAT/itemStream
maprcli stream topic delete -path /apps/1MBDA/1CAT/itemStream -topic itemDetail
maprcli stream topic list -path /apps/1MBDA/1CAT/itemStream
maprcli stream topic create -path /apps/1MBDA/1CAT/itemStream -topic itemDetail
