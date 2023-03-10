Function Filter-ObjectExcludeProperty
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [Array]$Data,
            [Parameter(mandatory)]
                [array]$ExcludeProperty
         )

    $Data = $Data | Select-Object * -ExcludeProperty $ExcludeProperty
    Return $Data
}

