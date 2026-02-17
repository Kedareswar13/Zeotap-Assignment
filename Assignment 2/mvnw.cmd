@echo off
@setlocal

if not "%JAVA_HOME%" == "" goto OkJHome
where java >nul 2>nul
if ERRORLEVEL 1 goto NoJava
set MAVEN_JAVA_EXE=java
goto init

:NoJava
echo Error: java.exe not found on PATH and JAVA_HOME not set. >&2
exit /B 1

:OkJHome
if exist "%JAVA_HOME%\bin\java.exe" (
  set MAVEN_JAVA_EXE="%JAVA_HOME%\bin\java.exe"
  goto init
)

echo Error: JAVA_HOME is invalid: %JAVA_HOME% >&2
exit /B 1

:init
set MAVEN_PROJECTBASEDIR=%CD%
if exist "%MAVEN_PROJECTBASEDIR%\.mvn" goto baseDirOk
set EXEC_DIR=%CD%
set WDIR=%EXEC_DIR%
:findBaseDir
if exist "%WDIR%\.mvn" (
  set MAVEN_PROJECTBASEDIR=%WDIR%
  cd "%EXEC_DIR%"
  goto baseDirOk
)
cd ..
if "%WDIR%"=="%CD%" (
  set MAVEN_PROJECTBASEDIR=%EXEC_DIR%
  cd "%EXEC_DIR%"
  goto baseDirOk
)
set WDIR=%CD%
goto findBaseDir

:baseDirOk
set WRAPPER_JAR="%MAVEN_PROJECTBASEDIR%\.mvn\wrapper\maven-wrapper.jar"
set WRAPPER_LAUNCHER=org.apache.maven.wrapper.MavenWrapperMain
set DOWNLOAD_URL=https://repo.maven.apache.org/maven2/org/apache/maven/wrapper/maven-wrapper/3.3.2/maven-wrapper-3.3.2.jar

for /F "usebackq tokens=1,2 delims==" %%A in ("%MAVEN_PROJECTBASEDIR%\.mvn\wrapper\maven-wrapper.properties") do (
  if "%%A"=="wrapperUrl" set DOWNLOAD_URL=%%B
)

if exist %WRAPPER_JAR% goto run

powershell -Command "&{ $wc = New-Object System.Net.WebClient; [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; $wc.DownloadFile('%DOWNLOAD_URL%','%WRAPPER_JAR%') }"
if not exist %WRAPPER_JAR% (
  echo Failed to download maven-wrapper.jar >&2
  exit /B 1
)

:run
%MAVEN_JAVA_EXE% -classpath %WRAPPER_JAR% "-Dmaven.multiModuleProjectDirectory=%MAVEN_PROJECTBASEDIR%" %WRAPPER_LAUNCHER% %*
exit /B %ERRORLEVEL%
