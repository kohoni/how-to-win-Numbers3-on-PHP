<?php

try {

    // 接続
    $pdo = new PDO('sqlite:rand.db');

    // SQL実行時にもエラーの代わりに例外を投げるように設定
    // (毎回if文を書く必要がなくなる)
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    // デフォルトのフェッチモードを連想配列形式に設定 
    // (毎回PDO::FETCH_ASSOCを指定する必要が無くなる)
    //$pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);

    // テーブル作成
    //this execrutes elemental table. Other table bases for this table. 
    $pdo->exec("CREATE TABLE IF NOT EXISTS raw_data(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        raw1 INTEGER,
        raw2 INTEGER,
        raw3 INTEGER,
        raw TEXT,
        boxtext TEXT,
        box1 INTEGER,
        box2 INTEGER,
        box3 INTEGER,
        boxint INTEGER
    )");

    //this table is boxnum.
    //if boxnum == boxint,then I get prize!
    $pdo->exec("CREATE TABLE IF NOT EXISTS boxids(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        boxnum INTEGER,
        boxnum1 INTEGER,
        boxnum2 INTEGER,
        boxnum3 INTEGER
    )");

    //this table is flag.
    //this is the main contents for progressing automatical winning numbers. id intends on raw_data's id. 
    $pdo->exec("CREATE TABLE IF NOT EXISTS flag_data(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        flag1 INTEGER,
        flag2 INTEGER,
        flag3 INTEGER,
        blank1 INTEGER,
        blank2 INTEGER,
        blank3 INTEGER,
        lag1 INTEGER,
        lag2 INTEGER,
        lag3 INTEGER,
        iprize INTEGER
    )");

    //insert in average. so I can sleep....
    $pdo->exec("CREATE TABLE IF NOT EXISTS result(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        maxlag1 REAL,
        maxlag2 REAL,
        maxlag3 REAL,
        maxblank1 REAL,
        maxblank2 REAL,
        maxblank3 REAL
    )");

} catch (Exception $e) {

    echo $e->getMessage() . PHP_EOL;

}

?>
