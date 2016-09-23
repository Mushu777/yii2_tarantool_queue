<?php

// ensure we get report on all possible php errors
error_reporting(-1);

define('YII_ENABLE_ERROR_HANDLER', false);
define('YII_DEBUG', true);
$_SERVER['SCRIPT_NAME']     = '/' . __DIR__;
$_SERVER['SCRIPT_FILENAME'] = __FILE__;

require_once(__DIR__ . '/../vendor/autoload.php');
require_once(__DIR__ . '/../vendor/yiisoft/yii2/Yii.php');

Yii::setAlias('@yiiunit/extensions/queue', __DIR__);
Yii::setAlias('@yii/queue', dirname(__DIR__));

Yii::setAlias('@yii/tarantool', dirname(__DIR__) . '/vendor/tass/yii2-tarantool');
