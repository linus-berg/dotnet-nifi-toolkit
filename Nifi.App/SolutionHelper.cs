namespace Nifi.App;

public static class SolutionHelper
{
  public static string GetSolutionDirectory()
  {
    // Start from the directory where the compiled binaries are running
    DirectoryInfo? current_directory = new DirectoryInfo(AppContext.BaseDirectory);

    // Walk up the directory tree until we find a .sln file
    while (current_directory != null && !current_directory.GetFiles("*.sln").Any())
    {
      current_directory = current_directory.Parent;
    }

    // If we reached the root drive and found nothing, return null
    if (current_directory == null)
    {
      throw new DirectoryNotFoundException("Could not find the solution directory. No .sln file found in the directory tree.");
    }

    return current_directory.FullName;
  }
}