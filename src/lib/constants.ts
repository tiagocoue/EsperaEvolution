// src/lib/constants.ts
export const DIRECAO = {
  IN: 'recebida',
  OUT: 'enviada',
} as const;

export type Direcao = typeof DIRECAO[keyof typeof DIRECAO];
