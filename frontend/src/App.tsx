import { useState } from "react";
import { NOCDashboard } from "./components/NOCDashboard";
import { LoginPage } from "./components/LoginPage";
import { getStoredToken } from "./api/client";

function isTokenValid(token: string | null): boolean {
  if (!token) return false;
  try {
    const payload = JSON.parse(atob(token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/")));
    return typeof payload.exp === "number" && payload.exp * 1000 > Date.now();
  } catch {
    return false;
  }
}

export default function App() {
  const [authed, setAuthed] = useState(() => isTokenValid(getStoredToken()));

  if (!authed) {
    return (
      <LoginPage
        onLogin={() => setAuthed(true)}
      />
    );
  }

  return <NOCDashboard onLogout={() => setAuthed(false)} />;
}
