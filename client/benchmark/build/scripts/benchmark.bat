@if "%DEBUG%" == "" @echo off
@rem ##########################################################################
@rem
@rem  benchmark startup script for Windows
@rem
@rem ##########################################################################

@rem Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

set DIRNAME=%~dp0
if "%DIRNAME%" == "" set DIRNAME=.
set APP_BASE_NAME=%~n0
set APP_HOME=%DIRNAME%..

@rem Add default JVM options here. You can also use JAVA_OPTS and BENCHMARK_OPTS to pass JVM options to this script.
set DEFAULT_JVM_OPTS=

@rem Find java.exe
if defined JAVA_HOME goto findJavaFromJavaHome

set JAVA_EXE=java.exe
%JAVA_EXE% -version >NUL 2>&1
if "%ERRORLEVEL%" == "0" goto init

echo.
echo ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto init

echo.
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME%
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:init
@rem Get command-line arguments, handling Windows variants

if not "%OS%" == "Windows_NT" goto win9xME_args
if "%@eval[2+2]" == "4" goto 4NT_args

:win9xME_args
@rem Slurp the command line arguments.
set CMD_LINE_ARGS=
set _SKIP=2

:win9xME_args_slurp
if "x%~1" == "x" goto execute

set CMD_LINE_ARGS=%*
goto execute

:4NT_args
@rem Get arguments from the 4NT Shell from JP Software
set CMD_LINE_ARGS=%$

:execute
@rem Setup the command line

set CLASSPATH=%APP_HOME%\lib\client-benchmarks-5.0.0-alpha6-SNAPSHOT.jar;%APP_HOME%\lib\commons-math3-3.2.jar;%APP_HOME%\lib\rest-5.0.0-alpha6-SNAPSHOT.jar;%APP_HOME%\lib\client-benchmark-noop-api-5.0.0-alpha6-SNAPSHOT.jar;%APP_HOME%\lib\elasticsearch-5.0.0-alpha6-SNAPSHOT.jar;%APP_HOME%\lib\transport-5.0.0-alpha6-SNAPSHOT.jar;%APP_HOME%\lib\transport-netty3-5.0.0-alpha6-SNAPSHOT.jar;%APP_HOME%\lib\transport-netty4-5.0.0-alpha6-SNAPSHOT.jar;%APP_HOME%\lib\reindex-5.0.0-alpha6-SNAPSHOT.jar;%APP_HOME%\lib\lang-mustache-5.0.0-alpha6-SNAPSHOT.jar;%APP_HOME%\lib\percolator-5.0.0-alpha6-SNAPSHOT.jar;%APP_HOME%\lib\httpclient-4.5.2.jar;%APP_HOME%\lib\httpcore-4.4.5.jar;%APP_HOME%\lib\httpasyncclient-4.1.2.jar;%APP_HOME%\lib\httpcore-nio-4.4.5.jar;%APP_HOME%\lib\commons-codec-1.10.jar;%APP_HOME%\lib\commons-logging-1.1.3.jar;%APP_HOME%\lib\lucene-core-6.2.0.jar;%APP_HOME%\lib\lucene-analyzers-common-6.2.0.jar;%APP_HOME%\lib\lucene-backward-codecs-6.2.0.jar;%APP_HOME%\lib\lucene-grouping-6.2.0.jar;%APP_HOME%\lib\lucene-highlighter-6.2.0.jar;%APP_HOME%\lib\lucene-join-6.2.0.jar;%APP_HOME%\lib\lucene-memory-6.2.0.jar;%APP_HOME%\lib\lucene-misc-6.2.0.jar;%APP_HOME%\lib\lucene-queries-6.2.0.jar;%APP_HOME%\lib\lucene-queryparser-6.2.0.jar;%APP_HOME%\lib\lucene-sandbox-6.2.0.jar;%APP_HOME%\lib\lucene-spatial-6.2.0.jar;%APP_HOME%\lib\lucene-spatial-extras-6.2.0.jar;%APP_HOME%\lib\lucene-spatial3d-6.2.0.jar;%APP_HOME%\lib\lucene-suggest-6.2.0.jar;%APP_HOME%\lib\securesm-1.1.jar;%APP_HOME%\lib\jopt-simple-5.0.2.jar;%APP_HOME%\lib\hppc-0.7.1.jar;%APP_HOME%\lib\joda-time-2.9.4.jar;%APP_HOME%\lib\joda-convert-1.2.jar;%APP_HOME%\lib\snakeyaml-1.15.jar;%APP_HOME%\lib\jackson-core-2.8.1.jar;%APP_HOME%\lib\jackson-dataformat-smile-2.8.1.jar;%APP_HOME%\lib\jackson-dataformat-yaml-2.8.1.jar;%APP_HOME%\lib\jackson-dataformat-cbor-2.8.1.jar;%APP_HOME%\lib\t-digest-3.0.jar;%APP_HOME%\lib\HdrHistogram-2.1.6.jar;%APP_HOME%\lib\spatial4j-0.6.jar;%APP_HOME%\lib\jts-1.13.jar;%APP_HOME%\lib\log4j-api-2.6.2.jar;%APP_HOME%\lib\log4j-core-2.6.2.jar;%APP_HOME%\lib\log4j-1.2-api-2.6.2.jar;%APP_HOME%\lib\jna-4.2.2.jar;%APP_HOME%\lib\netty-3.10.6.Final.jar;%APP_HOME%\lib\netty-buffer-4.1.5.Final.jar;%APP_HOME%\lib\netty-codec-4.1.5.Final.jar;%APP_HOME%\lib\netty-codec-http-4.1.5.Final.jar;%APP_HOME%\lib\netty-common-4.1.5.Final.jar;%APP_HOME%\lib\netty-handler-4.1.5.Final.jar;%APP_HOME%\lib\netty-resolver-4.1.5.Final.jar;%APP_HOME%\lib\netty-transport-4.1.5.Final.jar;%APP_HOME%\lib\compiler-0.9.1.jar

@rem Execute benchmark
"%JAVA_EXE%" %DEFAULT_JVM_OPTS% %JAVA_OPTS% %BENCHMARK_OPTS%  -classpath "%CLASSPATH%" org.elasticsearch.client.benchmark.BenchmarkMain %CMD_LINE_ARGS%

:end
@rem End local scope for the variables with windows NT shell
if "%ERRORLEVEL%"=="0" goto mainEnd

:fail
rem Set variable BENCHMARK_EXIT_CONSOLE if you need the _script_ return code instead of
rem the _cmd.exe /c_ return code!
if  not "" == "%BENCHMARK_EXIT_CONSOLE%" exit 1
exit /b 1

:mainEnd
if "%OS%"=="Windows_NT" endlocal

:omega
