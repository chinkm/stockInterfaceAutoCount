' VBA Trigger Event Configuration Wrapper 

Private Sub btn_StockInterface_Click()
 
On Error GoTo btn_StockInterface_Click_Err
    
    Dim strScriptPath As String
    Dim strPythonExe As String
    Dim strParaFullPath As String
    Dim strParaCompanyName As String
    Dim strShellCommand As String
    
    If IsNull(Me.Maincompany) Then
        MsgBox "Select Company and Date parameters to proceed.", vbExclamation
        Exit Sub
    End If
    
    ' Assign environmental references dynamically
    strParaFullPath = """" & Application.CurrentProject.FullName & """"
    strParaCompanyName = """" & Me.Maincompany.Value & """"
    
    ' Standardize execution paths across project modules
    strScriptPath = "C:\DatabaseAutomation\Scripts\StockInterfaceAutoCount.py"
    strPythonExe = "python" ' Assumes python is available on standard local system PATH variables
    
    ' Standardized execution string payload formatting
    strShellCommand = "cmd.exe /c """ & strPythonExe & " """ & strScriptPath & """ " & strParaCompanyName & " " & strParaFullPath & """"
    
    Shell strShellCommand, vbNormalFocus
    
btn_StockInterface_Click_Exit:
    Exit Sub
    
btn_StockInterface_Click_Err:
    MsgBox "Automation Error encounter tracking: " & Error$, vbCritical
    Resume btn_StockInterface_Click_Exit

End Sub
