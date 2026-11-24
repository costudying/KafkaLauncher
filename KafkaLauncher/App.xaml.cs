using System;
using System.Windows;

namespace KafkaLauncher
{
    /// <summary>
    /// App.xaml에 대한 상호 작용 논리
    /// </summary>
    public partial class App : Application
    {
        protected override void OnStartup(StartupEventArgs e)
        {
            try
            {
                NativeKafkaLoader.EnsureLoaded();
            }
            catch (Exception ex)
            {
                MessageBox.Show(
                    "Kafka 네이티브 라이브러리(librdkafka) 초기화 중 오류가 발생했습니다:\n" + ex.Message,
                    "초기화 경고",
                    MessageBoxButton.OK,
                    MessageBoxImage.Warning);
            }

            base.OnStartup(e);
        }
    }
}
