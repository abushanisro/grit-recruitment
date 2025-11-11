import { useRouter } from "next/router";
import { useRef } from "react";

export function useQueryState<T>(
  key: string,
  defaultValue: T
): [T, (value: T) => void] {
  const router = useRouter();
  const isFirstUpdate = useRef(true);

  const setState = (value: T) => {
    const url = new URL(window.location.origin + router.asPath);
    url.searchParams.set(key, JSON.stringify(value));
    //NOTE : This is a hack to prevent all other updated after the first update from being added to the history stack
    if (isFirstUpdate.current) {
      router.push(url, undefined, {
        scroll: false,
        shallow: true,
      });
      isFirstUpdate.current = false;
    } else {
      router.replace(url, undefined, {
        scroll: false,
        shallow: true,
      });
    }
  };

  const value = router.query[key] as string | undefined;
  const state: T = value ? JSON.parse(value) : defaultValue;
  return [state, setState];
}
