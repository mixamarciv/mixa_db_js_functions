:: ===========================================================================
:: переходим в каталог запуска скрипта
::@SetLocal EnableDelayedExpansion
:: this_file_path - путь к текущему бат/bat/cmd файлу
@SET this_file_path=%~dp0

:: this_disk - диск на котором находится текущий бат/bat/cmd файл
@SET this_disk=%this_file_path:~0,2%

:: переходим в текущий каталог
@%this_disk%
CD "%this_file_path%\.."


:: ===========================================================================
:: задаем основные пути для запуска скрипта
@SET NODE_PATH=d:\program\nodejs
@SET GIT_PATH=d:\program\git
@SET PYTHON_PATH=d:\program\Python26


::@SET PATH=%WINDIR%;%WINDIR%\system32
@SET PATH=%PATH%;%PYTHON_PATH%;%GIT_PATH%;%GIT_PATH%\bin;%GIT_PATH%\cmd
@SET PATH=%PATH%;%NODE_PATH%
@SET PATH=%PATH%;%NODE_PATH%\node_modules\npm\node_modules
@SET PATH=%PATH%;%NODE_PATH%\node_modules\.bin
@SET PATH=%PATH%;.\node_modules\.bin;%this_file_path%\..\node_modules\.bin

@SET NODE_PATH=.

::@ECHO %PATH%
:: ===========================================================================


