Set fso = CreateObject("Scripting.FileSystemObject")
strPath = fso.GetParentFolderName(WScript.ScriptFullName)
Set WshShell = CreateObject("WScript.Shell")
WshShell.Run chr(34) & strPath & "\start_local.bat" & Chr(34), 0
Set WshShell = Nothing
