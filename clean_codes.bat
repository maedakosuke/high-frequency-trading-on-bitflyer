@echo off
for /R %%a in (*.py) do (
  echo yapf -i %%a
  yapf -i %%a
)
