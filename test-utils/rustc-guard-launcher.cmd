@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "WORKSPACE_ROOT=%%~fI"
set "CARGO_TARGET_DIR=%WORKSPACE_ROOT%\target\guard-host"
set "GUARD_EXE=%CARGO_TARGET_DIR%\debug\rustc-guard.exe"
if not exist "%GUARD_EXE%" (
  set "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER="
  pushd "%WORKSPACE_ROOT%"
  cargo --config "build.rustc-workspace-wrapper=''" run -q -p rustc-guard -- --warmup
  set "WARMUP_EC=%ERRORLEVEL%"
  popd
  if not "%WARMUP_EC%"=="0" (
    set "EC=%WARMUP_EC%"
    endlocal & exit /b %EC%
  )
)
set "VELOQ_GUARD_FROM_LAUNCHER=1"
set "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER="
"%GUARD_EXE%" %*
set "EC=%ERRORLEVEL%"
endlocal & exit /b %EC%
