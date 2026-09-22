declare module "unzipper" {
  export interface Entry {
    type: string;
    path: string;
    stream(): NodeJS.ReadableStream;
  }

  export interface Directory {
    files: Entry[];
  }

  export function Open: {
    file(filePath: string): Promise<Directory>;
  };

  const unzipper: {
    Open: typeof Open;
  };

  export default unzipper;
}
