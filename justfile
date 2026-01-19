set shell := ["powershell.exe", "-c"]

debug:
    cargo build
    cp "F:\vscode\jj-cdc\target\debug\jj.exe" "E:\Program Files\Rust\.cargo\bin\jjd.exe"

release:
    cargo build --release
    cp "E:\Program Files\Rust\.cargo\bin\jj.exe" "E:\Program Files\Rust\.cargo\bin\jj_old.exe"
    cp "F:\vscode\jj-cdc\target\release\jj.exe" "E:\Program Files\Rust\.cargo\bin\jj.exe"

remove:
    rm "E:\Program Files\Rust\.cargo\bin\jjd.exe"
