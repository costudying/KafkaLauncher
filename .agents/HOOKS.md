# Lifecycle Hooks (`.agents/hooks.json`)

이 저장소는 Antigravity 커스터마이징 시스템의 라이프사이클 훅(`.agents/hooks.json`)을 포함합니다.

## 포함된 훅

### `xaml-syntax-guard`
- **이벤트**: `PostToolUse` (`matcher`: `edit_file|write_file`)
- **스크립트**: [.agents/scripts/check-xaml.ps1](./scripts/check-xaml.ps1)
- **역할**: XAML 파일(`MainWindow.xaml`, `App.xaml`)이 수정될 때 XML 구문 오류(예: XML 주석 내 `--` 사용으로 인한 `MC3000` 빌드 오류)를 즉시 사전 검증합니다.
- **기본 상태**: `"enabled": false` (저장소를 클론한 환경에서 의도치 않은 자동 스크립트 실행을 방지하기 위해 옵트인으로 설정되어 있습니다.)

## 활성화 방법

`.agents/hooks.json`에서 `"enabled"` 값을 `true`로 변경하세요.

```json
{
  "xaml-syntax-guard": {
    "enabled": true,
    ...
  }
}
```

## 입출력 규약 참고
- 훅 스크립트는 `stdin`으로 camelCase JSON 페이로드(`toolCall`, `cwd`, `hookEventName` 등)를 수신합니다.
- `PostToolUse` 훅은 `stdout`으로 빈 JSON 객체 `{}`를 반환해야 합니다.
