import { fetchAuthSession } from 'aws-amplify/auth';

export async function authFetch(url: string, options: RequestInit = {}): Promise<Response> {
  const session = await fetchAuthSession();
  const token = session.tokens?.idToken?.toString();

  return fetch(url, {
    ...options,
    headers: {
      ...options.headers,
      'Authorization': token || '',
    },
  });
}
